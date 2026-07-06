# NAS-AI Retrieval Evaluation — Ground-Truth Question Set v1

**How to use:**
1. Correct/replace the domain details (marked ⚠ where I guessed) — filenames, targets, article topics must match real data.
2. Fill in "Expected" with the actual ground truth (you know the data; be specific).
3. Run each question in BOTH Ask tab and Chat tab. Record: routed collection, method, top result title, verdict.
4. Verdict scale: ✅ correct · 🟡 partially correct (right doc, wrong section / incomplete) · ❌ wrong · 💀 hallucinated (confident but false)

**Scoring sheet columns (add per question):** `Ask verdict | Chat verdict | Routed collection | Method | Notes`

*** edited this part on the chat_engine.py script due to this error: Error querying xmltest: runquerywithmethod() takes from 2 to 7 positional arguments but 8 were given.  THe code change is:
	| changed this (lines 273-276:
		executor.submit(
                run_query_with_method,
                col, question, "best", 25, True, True, True, True
            ): col

	| to this lines 273-282:
		executor.submit(
                run_query_with_method,
                collection=col,
                question=question,
                mode="best",
                limit=25,
                show_exact_links=True,
                show_related_topics=True,
                force_answer=True,
            ): col

---

## Category 1 — Direct Lookup (baseline, should be solid)

| ID | Question | Expected | Expected collection | Ask verdict | Chat verdict | Chat Routed collection | Method | Notes |
|---|---|---|---|---|---|---|---|
| DL-01 | what is FIX tag 22 | SecurityIDSource definition | xml_test | ✅ | ✅ | xml_test | structured_namespace_lookup | used to work earlier |
| DL-02 | what is gsact.txt | Goldman RECON mapping: 019_W_RECON_GOLDMAN_PRIO_PULL, /outgoing, etc. | recon_assist_file | ✅ | ✅ | recon_assist_file | identifier_lookup | 
| DL-03 | what is tag 35 in FIX | MsgType definition | xml_test | ✅ | ✅ | xml_test | structured_namespace_lookup | 
| DL-04 | jpm_activity.xlsx details | JPM mapping record ⚠ verify filename | recon_assist_file | 🟡 | ❌ | kb_docs | semantic | chat completetly missed the collection.  Ask mentiones the file is mentioned but no data.
| DL-05 | what is the PB filename for gsact.txt | SRPB_4000_..._Custody_Tra... alias | recon_assist_file | ❌ | ❌| recon_assist_file | semantic | answers: does not provide a specific PB filename for "gsact.txt". The closest matches listed are "gsrepopos.txt", "gsactfx.txt", "gsactsd.txt", and "gscfd_act.txt" in both Ask and Chat.  As follow up on chat: "prime broker name then" returns result.
| DL-06 | what is the Whirlpool galaxy | M51/NGC5194 catalog entry | astro_catalog | ❌ | ❌| astro_catalog | semantic | The Whirlpool galaxy is mentioned in the entity record. However, specific details about it are not provided in the retrieved data. The document does not contain further descriptions or characteristics of the Whirlpool galaxy.
| DL-07 | ARD_OPERATING_EXP_PER_ASM_ASK | BBG field A0356 definition | bbg_fields | ✅ | ✅ | bbg_fields | semantic | 
| DL-08 | message broadcaster down | Broadcaster Down / Alert Checklist KB article | kb_docs | ✅ | 🟡💀 | none. -asked for follow up | semantic | It looks like you're experiencing an issue with your message broadcaster being down. To help troubleshoot this, can you provide a bit more context? For example:
	Are we talking about a specific system in your organization?
	What type of message broadcaster is it (e.g., email server, messaging app, etc.)?
	When did the problem start?
	Have there been any recent changes or updates to the system?
	This information will help me assist you more effectively!
| DL-09 | how to merge a ticket in HaloITSM | Merge ticket KB article | kb_docs | ✅ | ✅ | kb_docs | semantic | 
| DL-10 | NGC 2064 | catalog entry (type, coordinates) | astro_catalog | 🟡 | 🟡 | astro_catalog | lexical short | answer in both: The retrieved data does not contain specific information about NGC 2064. It mentions "REFLECTION NEBULA" but does not provide details related to NGC 2064.
-

## Category 2 — Paraphrase / Messy Phrasing (real-user robustness)

| ID | Question | Expected | Expected collection | Ask verdict | Chat verdict | Chat Routed collection | Method | Notes |
|---|---|---|---|---|---|---|---|
| PP-01 | that goldman activity file, whats the tidal job for it | gsact.txt → 019_W_RECON_GOLDMAN_PRIO_PULL | recon_assist_file | ❌ | 🟡 | kb_docs | semantic | Ask: The specific information about "the Goldman activity file" and its corresponding "tidal job" is not found in the retrieved data. The closest files mentioned are gsrepopos.txt, gscfdact.txt, gsactsd.txt, gsact.txt, and gsdcm_cash.txt, but none contain the exact query details you requested.  Chat: The tidal job for the Goldman activity file is part of the "RECONDAILYBATCH" group in Tidal. The specific job name includes the prime broker's name, so you should look for a job named with "Goldman" included.
| PP-02 | whats teh fix tag for order quantity ⚠ typo intentional | OrderQty tag 38 | xml_test | ✅ | ✅| xml_test | semantic | |
| PP-03 | brodcaster acting up agian ⚠ typos intentional | broadcaster troubleshooting | kb_docs/obsidian | ❌ | ❌💀 | obsidian | semantic | Ask article: FIX Brokers Appear Red in Broker Bar - User Permissioned.   Chat: he broadcaster issue is not directly addressed in the provided daily tasks schedule. The document outlines recurring tasks for server checks and other operations but does not mention any troubleshooting steps for a broadcasting problem. Please refer to specific broadcasting support documentation or contact technical support for assistance with this issue.  full text: Source: Obsidian Notes Corbin Daily Tasks Table for recurring daily tasks: Category: Corbin
| PP-04 | CR wont let me cancel a fix trade | How to Cancel FIX Trades in CRD When Cancel Action Is Blocked | kb_docs | ✅ | ❌ | obsidian | semantic | Chat: Asia FIX Orders and End of Day Processing |
| PP-05 | the us1 proc server thing for recon | 4.2 Checking Files on us1-proc02 / KB 2.2 | obsidian/kb_docs | 🟡 | 🟡 | obsidian | semantic | both show the 4.1 tidal doc, and state that i should refer to the 4.2 correct article.  Adding a link to suggested notes makes sense. |
| PP-06 | goldman prio pull job | 019_W_RECON_GOLDMAN_PRIO_PULL record | recon_assist_file | ❌| 🟡 | kb_docs | semantic | Ask: The term "goldman prio pull job" does not appear in the retrieved data. The closest related files are named without direct relevance: "gsrepopos.txt", "gsactsd.txt", "gsact.txt", "gscfdact.txt", and "gsd_act.txt". Please provide more details for a precise answer. Chat: 2.5 TIDAL Recon - Bad Dates - Clearing Lock Files |
| PP-07 | jennison morning stuff failing | Jennison Morning Batch escalation/runbook | kb_docs | ✅| 🟡| obsidian | semantic | chat: UAT Midle Tier Access - correct category|
| PP-08 | that lock file error on recon jobs | lock file clearing procedure (2.5 Clearing Lock Files) | kb_docs | ✅| ✅ | obsidian | semantic | |

## Category 3 — Procedural (how-to routing)

| ID | Question | Expected | Expected collection | Ask verdict | Chat verdict | Chat Routed collection | Method | Notes |
|---|---|---|---|
| PR-01 | how can I check if gsact.txt is on the sftp | 4.3.1 Checking sFTP / KB 2.3 sFTP article (NOT just the mapping record) | obsidian/kb_docs | 🟡 | ✅ | obsidian | semantic | Ask: lists the answer as steps but the last step skips so the login to sFTP, find the right folder then do the ls -lta.  it went from type cd /bin to ls -lta file name, skipping all previous steps.  the show full entry is good.|
| PR-02 | recon file missing, what do I do | Bad Dates workflow steps 1-4 | kb_docs/obsidian | ✅ | ✅ | kb_docs | semantic | Ask: If you encounter a missing recon file, it is most likely because "the brokers didn’t send their files." The procedure states, "Patrick Pingaro will reach out to the brokers and ask them to drop their files. We can rerun the tidal job corresponding to that broker (pulling FTP)." Follow this process when a file is missing.  Chat very similar.  it is a good overall summary, not a step by step.|
| PR-03 | how to rerun a tidal recon job | 4.1 Checking the Tidal Recon Job | obsidian | ❌ | ❌ | kb_docs | semantic | Ask & Chat: 2.5 TIDAL Recon - Bad Dates - Clearing Lock Files|
| PR-04 | how to manually load a file in the recon tool | 5. Manual File Loading in RECON Tool | obsidian | ❌ | ❌ | kb_docs | semantic | both: 2.4 TIDAL Recon - Bad Dates - Step 4: Pulling Files Manually from Recon (DRAFT) |
| PR-05 | weekend restart procedure for moore prod | Automated 21R2 Weekend Restart (PROD, not DEV/QA) | kb_docs | ✅ | ✅ | kb_docs | semantic | correct answer but is too summarized. Chat: To perform the weekend restart procedure for Moore PROD:
	Check that you receive success/failure emails for the weekly restart job group at email@com, specifically from email@com for PROD.
	If the Tidal Job CRDWEEKLYRESTART ran successfully and completed normally, proceed to 'Server Checks'.
	Log in to your assigned Moore machine via NetScaler AAA using your Moore username and password.
	Access the PROD CRD Web Admin at us1-crd02:8081/crts/, log in with individual user credentials, and verify that all xx active CharlesRiver services are running as specified (quoted verbatim from the document).|

| PR-06 | how do I copy FIX logs to my machine | How to copy FIX Logs to local machine | kb_docs | ✅ | ❌| | obsidian | semantic | Chat: did not find |
| PR-07 | steps for one madison data load failure | One Madison Data Load Status FAILURE article | kb_docs | ✅ | ✅ | obsidian | semantic| |
| PR-08 | how to check charles river logs on a user machine | CR Log Folder for Errors note (C:\Program Files...) | obsidian | ❌ | ❌ | kb_docs| semantic | Ask & Chat: CRD Logs Archive. Chat: |

## Category 4 — Aggregation (known structural gap — discovery routes to retrieval, not SQL)

| ID | Question | Expected | Notes | Ask verdict | Chat verdict | Chat Routed collection | Method | Notes |
|---|---|---|---|---|---|---|---|
| AG-01 | how many KB articles are there | 178 (active) | true SQL: COUNT on kb_docs distinct articles | ✅ | ✅ | kb_docs | discovery count | Ask: "There are 4 matching records.". Chat: "There are 4 matching records." |
| AG-02 | how many articles mention FIX | ⚠ run SQL to get truth: `SELECT COUNT(DISTINCT primary_name) FROM chunks WHERE collection_name='kb_docs' AND nlp_text ILIKE '%fix%'` | filtered count | ✅ | ✅ | kb_docs | discovery count | Ask & Chat: 0 matching records |
| AG-03 | how many FIX tags are there | 947 (namespace=tag) | xml_test | ✅ | ✅ | xml_test | discovery count | Ask& Chat: "There are 41 matching records."| 
| AG-04 | how many Goldman files are in the recon mapping | ⚠ verify: count type=Goldman in recon_assist_file (16?) | filtered count | ✅ | ✅ | recon_assist_file | discovery count | both: The number of Goldman files in the recon mapping is 16. |
| AG-05 | how many images do I have for M42 | ⚠ verify in astro_test (likely 1 in test set) | filtered count | ✅ | ❌ | obsidian | discovery count | Ask: You have 1 image for M42. Chat: You have 0 images for M42.
| AG-06 | how many galaxies are in the catalog | ⚠ verify: count type=Gx in astro_catalog | filtered count | ❌ | ❌ | astro_catalog | discovery count | Both: The retrieved data does not provide information on the number of galaxies in the catalog. It simply states: "Found 0 matching records." This indicates there may be an issue with the query or the dataset itself. |
| AG-07 | list all prime brokers in the recon file | BOA, BONY, CHASE, CITCO, CITI, Credit Suisse, Deutsche, Goldman, JPM, MIZUHO, Morgan | distinct values | ✅ | ✅ | recon_assist_file | discovery list | Ask: 0 match(es). Chat: The retrieved data does not contain any information about prime brokers listed in the recon file. Found 0 item(s): indicates no items were found matching the search criteria. |
| AG-08 | which broker has the most recon files | ⚠ verify: GROUP BY type ORDER BY count | group-by | ✅ | ✅ | recon_assist_file| semantic | Ask: The retrieved data does not contain information about which broker has the most recon files. It lists several file names but does not provide details on their quantity or associated brokers. Chat: The retrieved data does not specify which broker has the most recon files. It only lists file names without mentioning their quantity or associated brokers.|
| AG-09 | how many images with gain 100 | ⚠ verify astro_test file_gain values | metadata filter | 🟡 | ❌ | obsidian | discovery_count | Ask: The retrieved data does not specify the number of images with a gain of 100. It only indicates that there is "1 matching record," but it does not provide details about the gain setting of this image. Chat: The retrieved data does not specify the number of images with a gain of 100. It only states: "Found 4 matching records." Without further details linking these records to images having a gain of 100, the exact number cannot be determined from this information alone. |
| AG-10 | how many fields are in FIX 4.4 | ⚠ verify: componentid count for FIX44 source file | filtered count | ❌| ❌| xml_test | discovery count | Both: The retrieved data does not specify the number of fields in FIX 4.4. It only indicates that there are 22 matching records, which is insufficient to determine the total number of fields in FIX 4.4. |

## Category 5 — Cross-Collection (needs 2+ sources)

| ID | Question | Expected | Collections |Ask verdict | Chat verdict | Chat Routed collection | Method | Notes |
|---|---|---|---|---|---|---|---|
| XC-01 | I'm missing gsact.txt from Goldman, what can I do | mapping record + Tidal/sFTP check procedure | recon + obsidian/kb | ✅ | 🟡 | recon_assist_file | semantic | Chat: pulled the list from recon_assist_file. Ask: To find the original "gsact.txt" from Goldman Sachs, you need to locate the .pl script used during the FTP process for renaming files. According to the document:
	Go to Tidal → RECON (under job activity) → REPORTS → RECON → RECONDAILYBATCH → PBRECONFILE_RETRIEVAL → The job of the specific prime broker (in this case, Goldman).
	Check the file the job is running in the Dependencies section.
	On US1-PROC02 Desktop → K Recon Folder → FTP, find and open GSCopy.bat.
	Inside GSCopy.bat, identify the .pl script being used (e.g., GSMove.pl).
	Open the .pl script located at K Recon folder → Perl.
	Within this .pl file, you will see mappings of prime broker filenames to Geneva names. Look for "gsact.txt" in these mappings.
	The document does not specify exactly what Goldman's original filename is for "gsact.txt", but following these steps should help you identify it.|
| XC-02 | what tidal job pulls jpm files and how do I check if it ran | JPM job name + 4.1 checking procedure | recon + obsidian | ❌| ❌| kb_docs | semantic | Ask: The retrieved data does not specify which tidal job pulls JPM files or how to check if it ran. It lists file names related to JPM but lacks information on the specific tidal job or verification process. Chst: 2.1 - TIDAL Recon - Bad Dates - Step 1: Tidal Job Checks (DRAFT)|
| XC-03 | what is tag 38 and are there KB articles about FIX order issues | tag def + FIX-related KB articles | xml_test + kb_docs | | 🟡 | xml_test | | Chat retuened the tag, and then :" Regarding KB articles about FIX order issues, there is no specific mention in the retrieved data provided. To find such articles, you may want to check the knowledge base directly or search for "FIX order issues KB articles" on your preferred search engine." |
| XC-04 | show me the M51 catalog entry and do I have images of it | catalog entry + M51 collection images ⚠ verify | astro_catalog + M51 | | 🟡 | M51 | | Chat: The retrieved data does not contain a specific M51 catalog entry but shows multiple image file entries for observations of the M51 galaxy. You have images of M51, as indicated by these lines:
	"lightm511800sbin130023: m51"
	"lightm511800sbin130001: m51"
	"lightm511800sbin130021_20220501110838: m51"
	"lightm511800sbin130002_1: m51"
	"lightm511800sbin130032: m51"
	Each entry specifies the camera and mount used, along with an exposure time of 180.0 seconds. |
| XC-05 | bad dates alert for citi, which file and what steps | citi mapping + bad dates workflow | recon + kb_docs | | ❌ | | kb_docs | in full entry, see it picjed: Jennison SOD Checkout and Data Verification (Daily 6-6:30am) |
| XC-06 | broadcaster is down, who do I contact and what do I check | checklist article + contacts note ⚠ verify contacts location | kb_docs + obsidian | | Not sure snce kb and obsidian are exact replica| | | |

## Category 6 — No-Answer Traps (hallucination detection)

| ID | Question | Expected behavior | Chat verdict | Notes |
|---|---|---|
| NA-01 | what is FIX tag 99999 | "not found" — tag doesn't exist | ✅ | The retrieved data does not contain information about FIX tag 99999. The closest results provided are for tags 371, 258, 141, and 232, but none of these match the requested tag number. There is no mention of tag 99999 in the given document.|
| NA-02 | what is the recon mapping for barclays_fx_swap.txt ⚠ confirm this file does NOT exist | "not found" — must not invent a Tidal job | ✅| The specific recon mapping for "barclaysfxswap.txt" is not found in the retrieved data. The closest files mentioned are mscfdact.txt, gsbalfx.txt, gsactfx.txt, gsrepopos.txt, and csfbact.txt, but they do not provide information on barclaysfx_swap.txt., but it answered for known filenames earlier same answer|
| NA-03 | how do I restart the Bloomberg terminal server | no such procedure in any collection — "not found" | ✅ | The retrieved data does not contain instructions on how to restart the Bloomberg terminal server. It provides contact information for various FIX connection support teams but lacks details on restarting the Bloomberg terminal server. Please refer to your local IT support or contact Bloomberg directly for assistance with this task. | 
| NA-04 | what are the FIX 5.0 SP2 changes | only 4.2/4.4 ingested — must not fabricate | ✅ | The retrieved data does not contain any information about the changes in FIX 5.0 SP2. |

## Category 7 — Multi-Item (known gap CODE-023 — measure severity)

| ID | Question | Expected | Chat verdict | Notes |
|---|---|---|
| MI-01 | what are tags 22, 35 and 54 | all three definitions (SecurityIDSource, MsgType, Side) | ❌ | The retrieved data does not contain information about tags 22, 35, and 54. It provides details for other tag numbers but not the ones specifically asked for. It then provided 45 matching tags and description.|
| MI-02 | give me the tidal jobs for gsact.txt and gspos.txt | both Goldman job names | ❌| The retrieved data does not contain any tidal jobs for "gsact.txt" and "gspos.txt". Found 0 item(s). |
| MI-03 | compare FIX tag 38 and tag 152 | OrderQty vs CashOrderQty — both defs | 🟡| fou nd tag 38, but not tag 152| 
| MI-04 | what are the moore filenames for goldman and jpm activity | both mappings |

---

## Results Summary (fill after run)

| Category | Ask ✅ | Chat ✅ | Notes |
|---|---|---|---|
| 1 Direct Lookup ( /10) | | | |
| 2 Paraphrase ( /8) | | | |
| 3 Procedural ( /8) | | | |
| 4 Aggregation ( /10) | | | |
| 5 Cross-Collection ( /6) | | | |
| 6 No-Answer ( /4) | | | |
| 7 Multi-Item ( /4) | | | |
| **Total ( /50)** | | | |

## Failure Buckets (fill after run — this drives the priority list)

- Routing wrong (wrong collection):
- Ranking wrong (right collection, wrong doc):
- Synthesis wrong (right doc, mangled answer):
- Aggregation structural (discovery→retrieval instead of SQL):
- Cross-link gap (related info not surfaced):
- Hallucination (💀 count):
