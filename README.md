# slaysybil

# Team 
We are a team of web3 data scientists aiming at preventing sybil attacks. Before the Gitcoin Hackthon, we have already done a lot of preparations and related works, such as the Sybil analysis on HOP and Gnosis Safe airdrop. We position our work as the algorithmic detection LEGO in a Sybil resistance system although it means high development and maintenancen cost

# Deliverables
## Our Tech Report
We strongly suggest you to download the $\color{green}{antisybil-V5.pptx} $, our comprehensive 43-page tech report. The ppt contains our thinking, logic, methodology, algorithms and reasoning on a bunch of cases. We propose and develop four approaches, namely (1) bulk transfers pattern mining, (2) bulk donations pattern mining, (3) sequential behavior pattern mining and (4) asset-transfer graph mining for slaying sybil. These approaches form a systematic algorithmic LEGO and totally find $\color{green}{16,994}$ High Risk Sybils. Besides, we make our first attempt to detect $\color{green}{grant}$ $\color{green}{fraud}$ and find one case. 

## The Data Directory

Our data are uploaded to github and aliyun OSS.

### github/data

（1）data/sybilRecognition/atg：data warehouse details for sybil detection using the asset-transfer graph approach,

（2）data/typicalCases/ATG：gephi project files for visualizing examples of the asset-transfer graph approach(Page 33,34,35,37,38),

（3）data/typicalCases/bulkDonation: datasets as to the examples of bulk donations(page 20 and 21),

（4）data/typicalCases/GrantFraud：datasets as to the example of grant fraud(page 40),

（5）data/overallResults：aggregate the Sybils discovered from different approaches into one table safe_tmp.leo_result_address_final.csv;

### aliyun oss（https://oss.console.aliyun.com/bucket/oss-cn-hangzhou/trusta-gr15-hackathon/object）

（1）raw-data：Besides the raw data set from GR15, we collected data from alchemy, chainbase, dune, and zysync. Refer to the "Data Preparation" page (page 5) for the list of datasets, 

（2）ODS：The ODS layer stores raw data in the data warehouse. The data structure is basically consistent with that in the source system,

（3）DWD：the data warehouse details for modelling,

（4）result：aggregate the Sybils discovered from different approaches into one table safe_tmp.leo_result_address_final.csv (same as 0x9simon/slaysybil/data/overallResults)


## The Code Directory

(1) code/loadData: the code for loading all the possible datasets for modelling and mining.     

(2) code/sybilRecognition：the sql and python code for detecting sybils, including the bulkTransfer code, bulkDonation code, behavior code and asset-transfer graph (atg) code. In order to run the program, some necessary data sets are also provided. 

(3) code/typicalCase：the code to select typical examples for bulkDonation, “grantFraud”, “behavior.py” and “bulk_transfer.py”.

(4) code/overallResults：the code for combining the results from every approach into one comprehensive result.


## The Doc Dierectory

Mainly in doc/overallResults directory, there are some aggregted statistics as to bulk donations, bulk transfers, behavior and atg.  The final overall statistics is also included.


We have also uploaded the code to collect data, conduct feature engineering, compute risk score and Sybil clusters, and generate visualizations.

We keep on building an anti-sybil system. Look forward to collaborating with data scientists from Gitcoin community in Sybil hunting.
