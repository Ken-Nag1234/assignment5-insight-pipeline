# Prioritize playback reliability and control, with billing clarity as the next bet

_Written by `openai/gpt-6-luna` (prompt m4) from saved aggregates; every number was inserted by code from ranking.csv or computed facts. [C###] refers to claims.csv; `review IDs` refer to records.jsonl._

## Recommendation

Make playback reliability and control the primary product focus for next quarter, with a tightly scoped billing and Premium-access workstream alongside it. The broad playback issue has substantial priority 23681 [C001], and playback crashes are especially severe on average 3.757009 [C002]. Target skipping, wrong-track or queue behavior, and crashes; the examples illustrate these subtopics without representing every complaint in the issue. A user describes songs skipping on phone speakers `e3315f54-d116-45c3-a2ab-02782fbfe9a8`, while another reports repeated crashes `56497e20-4dad-40fd-b3c6-a300b3d0a239`.

Pair this with a focused audit of Premium entitlement, billing continuity, and feature-access messaging: Premium billing/access carries high priority 18973 [C003], including a report of continued billing after Premium disappeared `50456d9f-a97a-4f89-aa89-e90df29d68f0`. Treat usability as a targeted supporting track—not the sole headline—by reviewing ad experience and playlist/update regressions, both meaningful issue scopes 17525 [C004] 5456 [C005] 6492 [C006].

## Evidence

Playback accounts for 8472 complaints and 28842 severity points across its classified topic scope. The general playback issue ranks highly by priority 23681 [C001], while crashes have high mean severity 3.757009 [C002]. Samples point to distinct playback and control failures: songs playing other than the selected playlist `3f0e9733-92bf-410c-949a-2bf4d423b852`, and crashes during use `cfba5542-78a7-4ac5-a9c0-5293639a95a3`. These are examples, not explanations of the full issue totals.

Billing spans 10372 complaints and 29136 severity points; Premium billing and feature access is a major component 18973 [C003]. Usability is broader by complaint volume—18130 complaints and 45574 severity points—with ad experience 17525 [C004], playlist management and playback 5456 [C005], and post-update usability 6492 [C006] representing distinct subtopics. Ads are described as excessive `8b7ab125-8665-441c-a0a7-feb2abb5afcd`; a playlist example describes difficulty creating one `ca33cb8c-9d06-47b6-a50d-a50ed2a39828`.

Access login has fewer complaints than the leading playback and billing issues, but its mean severity is high 3.971487 [C007]; a sample reports repeated phone-number login failure `6c167086-261a-4c55-9289-2d4727019db4`. This supports monitoring and focused remediation, not necessarily making access the broad quarterly centerpiece.

## Alternatives considered

**Billing and Premium access first:** A credible alternative because Premium billing/access has high priority 18973 [C003], and billing spans 10372 complaints overall. Prefer it as a parallel, bounded workstream unless operational evidence shows entitlement or payment failures are driving greater harm than playback.

**Usability first:** Usability has the largest named product-area complaint volume in the supplied topic totals, 18130, and contains separate ad, playlist, and update concerns 17525 [C004] 5456 [C005] 6492 [C006]. Its breadth argues for targeted fixes rather than a single undifferentiated initiative; the ad examples alone do not establish the cause or impact of the aggregate.

**Access first:** Login complaints show high average severity 3.971487 [C007], but the access topic total is 2201. This supports a focused investigation of sign-in failures while the larger playback opportunity receives the main effort.

## Limitations

Coverage is incomplete: 660622 source reviews include 284709 completed classifications and 375900 pending reviews; the supplied scope note says counts cover completed classifications only. The complaint set contains 92035 records across 34 ranked issues. The catch-all `other` scope is large at 48342 complaints and 98177 severity points 98177 [C008], but it is unspecific and should not be treated as a product-area recommendation.

Labels are uncertain: verification topic agreement is 0.952 and accepted golden-topic accuracy is 0.82. Issue names are short display labels, and random sample quotes may be ambiguous or span multiple topics; they cannot establish the full issue count, prevalence of a particular subproblem, or root cause. The aggregates also do not establish business impact, implementation cost, or causal links. Validate proposed fixes against product telemetry and customer-support evidence before committing the quarter.
