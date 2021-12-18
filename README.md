# ThesisPortfolio
A collection of Python scripts that I used for my thesis.

**Thesis title:** Comparing the impact of the maximum amount of low-frequency and high-frequency liquidity benchmarks on stocks return

## [Step 1: Scraping trades and limit order book data from TSE](https://github.com/behnoud-bazrafshan/ThesisPortfolio/tree/main/Scraping)
* Scraped near 300 milion data (including trades and limit order book) from Tehran Stock Exchange website [**(sample scraped data)**](https://drive.google.com/drive/folders/1N4d34Zb1yxoOCJI0VOrJjYYptBzXjHge?usp=sharing)
## [Step 2: Clean and prepare data, then calculate liquidity measures](https://github.com/behnoud-bazrafshan/ThesisPortfolio/tree/main/Calculating%20liquidity%20measures)
* Calculated effective spread
* Calculated 5-minute price impact
* Calculated Amihud illiquidity measure
* Calculated monthly mean and max for each measure
## [Step 3: Portfolio analysis](https://github.com/behnoud-bazrafshan/ThesisPortfolio/blob/main/portfolio_analysis.ipynb)
* Compared high-frequency and low-frequency liquidity measures by portfolio sorting approach
## [Step 4: Robustness check](https://github.com/behnoud-bazrafshan/ThesisPortfolio/tree/main/Robustness%20Check)
* Calulated Fama Frech, Carhart, and Pastor Stambaugh risk factors
* Estimated alphas and corresponding t-statistics
