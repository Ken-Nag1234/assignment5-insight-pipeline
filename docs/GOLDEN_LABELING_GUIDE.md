# Golden-50 Labeling Guide

This guide is for the **human** labeler of the 50 golden reviews. The golden set is the
reference answer key used to evaluate the pipeline. The authoritative label definitions are in
[`GRADING_CONTRACT.md`](../GRADING_CONTRACT.md); this guide restates them as a step-by-step
procedure and adds conventions for the fields the contract leaves open.

## Ground rules

1. **Label by hand, from the review text only.** Do not ask a model, and do not look at any
   pipeline prediction for these reviews before you finish.
2. **Keep your answers away from the pipeline.** Golden labels are never used in prompts,
   few-shot examples, routing thresholds, issue discovery or prompt tuning. Prompt tuning uses
   `cost_100.csv`, `checkpoint_500.csv` and `analysis_10000.csv` only. If golden results ever
   influence a revision, we must disclose that and add fresh held-out cases for the final check.
3. **Do not edit `review_id`, `source_sha256` or `review_text`.** The validator rejects edits.
4. **Ignore stars and metadata.** The working file deliberately omits the star rating, likes,
   app version and timestamp so they cannot anchor your judgment.
5. **Do not guess missing context.** If impact is unclear, choose the most defensible label,
   set `needs_review` to `true`, and explain in `notes`.
6. **Label all 50.** Do not skip hard cases; mark them `ambiguous` instead.

## Files and commands

| Item | Path |
|---|---|
| Working file (fill this in) | `golden/golden_50_labels.csv` |
| Original blank template (do not edit) | `golden_50_to_label.csv` |

```sh
python3 -m pipeline.golden validate   # checks allowed values, ranges and exact quotes
```

The working file is created once with `python3 -m pipeline.golden template`; the command refuses
to overwrite an existing file. Edit it in a spreadsheet app or text editor and save it as **UTF-8 CSV**.
Spreadsheet apps can silently change text (for example, smart quotes or trimmed spaces). Run the
validator after every editing session.

## Recommended order for each review

1. Read the whole review.
2. Decide **intent** (precedence rules below).
3. Decide **topic** (the problem with the highest supported severity; ties go to the first mentioned).
4. Decide **severity** from the reported impact, not the tone.
5. Decide **sentiment**.
6. Copy an **evidence_quote** that supports the topic and intent.
7. List explicit **entities**.
8. Set **needs_review** and, when two answers are both defensible, **ambiguous** plus the `accepted_*` columns.

## Field reference

### `intent` — one of `cancellation`, `complaint`, `request`, `praise`, `unclear`

Apply the first rule that matches:

| Order | Intent | Use when |
|---|---|---|
| 1 | `cancellation` | The reviewer explicitly leaves, uninstalls, cancels, or threatens to |
| 2 | `complaint` | A negative experience is reported, including mixed praise and criticism |
| 3 | `request` | A desired change without a reported failure |
| 4 | `praise` | Positive experience only |
| 5 | `unclear` | Unrelated or meaningless text, or bare boycott slogans with no product complaint or personal departure |

A generic "bad app" is a `complaint`; do not infer a specific defect.

### `topic` — one of eight exact values

| Topic | Covers |
|---|---|
| `access` | Login, signup, password, account access |
| `usability` | Navigation, controls, layout, queue/playlist management, ad interruptions |
| `playback` | Playback failure, crashes, lag, connection failures, audio quality, resource use |
| `downloads` | Downloading, saved music, offline listening, disappearing downloads |
| `catalog` | Missing songs/artists, search/discovery, recommendations, lyrics availability |
| `billing` | Price, charges, subscriptions, paywalls, premium entitlement; explicitly premium-only controls |
| `support` | Contacting support and the support response |
| `other` | General praise/criticism, unrelated content, or no supported specific topic |

- Several problems: choose the one with the **highest supported severity**; on a tie, the **first mentioned**.
- Positive review: the **first specific praised feature**; general praise is `other`.
- Mentioning a paid plan alone is **not** `billing`. A subscription failing to activate is `billing`;
  music crashing for a paying customer is `playback`.

### `severity` — integer 1 to 5

| Level | Meaning |
|---|---|
| 1 | No reported problem: praise, neutral/unclear content, or a pure feature request |
| 2 | Dislike, generic criticism, minor annoyance, cosmetic issue; no supported functional loss |
| 3 | A degraded or restricted function; some use or a workaround remains |
| 4 | A clearly blocked core task, such as being unable to log in or play music |
| 5 | Explicit serious financial, privacy or data harm |

Stars, angry language, an expensive plan, a crash on its own, or cancellation intent do **not**
by themselves raise severity.

### `sentiment` — number from -1 to 1

Use one of five anchors unless the text clearly sits between them:

| Value | Meaning |
|---|---|
| `-1` | Strongly negative (hostile, furious, "worst ever") |
| `-0.5` | Negative |
| `0` | Neutral, unclear, or evenly mixed |
| `0.5` | Positive |
| `1` | Strongly positive ("love it", "best app") |

The evaluation compares sentiment with a numeric tolerance that we declare before scoring.

### `evidence_quote` — exact substring of `review_text`

- **Copy and paste** from `review_text`; never retype, translate, correct spelling, or change emojis or spaces.
- Choose the shortest span that supports the topic and intent. For a very short review, the whole text is fine.
- The validator checks exact substring membership.

### `entities` — JSON list of strings

- Product features or named things **explicitly mentioned** in the text, written in lowercase
  short form, for example `["offline mode", "shuffle"]`.
- Use `[]` when nothing specific is mentioned. Do not add implied features.

### `needs_review` — `true` or `false`

`true` when the label depends on missing context, the text is in a language you cannot read
reliably, or the evidence is too weak for a confident label.

### Ambiguity columns (evaluation only, not part of the grading export)

| Column | Format |
|---|---|
| `ambiguous` | `true` or `false` |
| `accepted_topics` | Other defensible topics, separated by `\|`, e.g. `usability\|billing` |
| `accepted_intents` | Other defensible intents, `\|`-separated |
| `accepted_severities` | Other defensible severities, `\|`-separated, e.g. `3\|4` |
| `notes` | Free-text reasoning, especially for ambiguous or `needs_review` cases |

Your primary label still goes in `topic`, `intent` and `severity`; the `accepted_*` columns only
list additional acceptable answers.

## Worked examples

These are **illustrative sentences written for this guide, not dataset reviews**. They show how
the shared definitions are applied.

| Example text | topic | intent | severity | sentiment | Why |
|---|---|---|---|---|---|
| "Downloaded songs stop playing when I go offline." | `downloads` | `complaint` | 3 | -0.5 | Offline listening fails; online use remains |
| "Can't log in since the update, it says wrong password every time." | `access` | `complaint` | 4 | -0.5 | Core task (login) clearly blocked |
| "Way too many ads, I'm uninstalling." | `usability` | `cancellation` | 2 | -0.5 | Ad interruptions are usability; cancellation does not raise severity |
| "I was charged twice for Premium this month and support never answered." | `billing` | `complaint` | 5 | -1 | Explicit financial harm outranks the support problem |
| "Shuffle-only playback unless you pay is ridiculous." | `billing` | `complaint` | 3 | -0.5 | Explicitly premium-only control; function restricted |
| "Please add a sleep timer for podcasts." | `usability` | `request` | 1 | 0 | Desired change, no reported failure |
| "Discover Weekly always finds me new artists I love!" | `catalog` | `praise` | 1 | 1 | First specific praised feature is recommendations |
| "Great app" | `other` | `praise` | 1 | 0.5 | General praise |
| "Bad app" | `other` | `complaint` | 2 | -0.5 | Generic criticism; no specific defect inferred |
| "Boycott Spotify!!!" | `other` | `unclear` | 1 | -0.5 | Bare boycott slogan, no product complaint or departure |
| "Premium user, but songs keep crashing." | `playback` | `complaint` | 3 or 4 | -0.5 | Paid plan alone is not billing; set `ambiguous=true`, `accepted_severities=3\|4` because it is unclear whether playback is fully blocked |

## When you finish

1. Run `python3 -m pipeline.golden validate` until it reports `valid=50` and no problems.
2. Record the date you finished and any rules you added in `notes` or in a short message.
3. Only then may the pipeline's golden-set evaluation be run.
