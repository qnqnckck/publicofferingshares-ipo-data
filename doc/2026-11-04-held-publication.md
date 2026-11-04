# Held-data review and scoped publication

## Idea / MVP / flow
User requests review of other held data and publication if valid, after known audit blockers were explained. Scope verified YKMC demand metrics and November3 observations for Dongwon,Baropharm,NexI. Clean HEAD -> reviewed canonical overrides/snapshots -> isolated regeneration -> scoped diff -> main -> exact public comparison. This manual scope is not a future heartbeat exception.

## Evidence and exclusions
YKMC primary20261103000181 and final terms20261103000312: price11000,institutions2376,competition1163.35,quantity lockup142125000/1971691000,float34.81%,postIPO14482531shares,marketcap159307841000. November3 broker actual timestamps/source URLs retained in canonical inputs; not final allotments. Latest16:08:39Dongwon238550,total1219.37,proportional2437.75; Baropharm52856,total19.27,proportional38.54; NexI16:08:34,15916,total1.23,proportional1.47.
Inventory also identifies five locally added issuers (CNM,JNTG,SFC,ANH,Shinyoung12), three migrated overrides (Ucast,Genon,Enki), Optonics and other older metric/retail drafts. These are excluded pending complete source/delta review; generated churn alone is not verified correction. Tool code/tests/README and unrelated index/worktree changes excluded. No claim that all held data is published.

## Validation
Re-read YKMC final-terms table November4:2376institutions,1971691000shares,total1163.35. Isolated468stock generation, published histories retained, non-target rows preserved; no schedule changes. Dart analysis passed. Full phase audit1error27warnings persists (IM SPAC1lockup); supplementary report build/completeness-20261104-held.json retains unresolved review errors. This scoped manual publication does not certify those excluded fields. JSON and staged whitespace gates required before commit; exact remote verification after push.
