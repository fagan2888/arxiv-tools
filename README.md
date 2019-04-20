## arXiv-keyword-searcher

#### Prerequisites

**Only tested with Python 3**

ArXiv provides [bulk data access](https://github.com/user/repo/blob/branch/other_file.md) through [Amazon S3](https://aws.amazon.com/s3). You need an account with [Amazon AWS](https://aws.amazon.com/free) to be able to download the data.


#### Downloading and search arXiv documents for keywords
1- Install [s3cmd](https://github.com/s3tools/s3cmd) which is a command line tool for interacting with S3

`pip install s3cmd`

2- Configure your s3cmd by entering credentials found in the account management tab of the Amazon AWS website

`s3cmd --configure`

3- Install pdfminer.six to get text from a pdf on the fly

```
pip install pdfminer.six
```

4- Search arxiv for particular keywords

For example, searching for "resnet", "googlenet" and "alexnet".
The keyword search is case-insensitive

```bash
python download.py --keywords "resnet,googlenet,alexnet"
```

We store the results database in a pickle file (Default: `db.pkl`).
When you run download.py again, it checks for this file and skips processing the files from arxiv that were already processed.