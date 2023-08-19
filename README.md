My Data Explorer
================

This will be a set of tools to explore your digital footprint and claim your data from web services.

You are supposed to run this code on _your_ data locally. Keep in mind that your data may contain sensitive information. Please be careful with sharing the processing results and exposing the app to someone else.

## Meet Your Personal Data

The idea of this effort is to make personal data accessible and make the process of its analysis effortless and enjoyable. Playing with _your data_ may have some practical outcomes.

- **Be aware**. How many accounts do you have? Is your data safe? How much surveillance are you exposed to? How do companies use your data? Is there an [alternative](https://github.com/pluja/awesome-privacy) service that respects privacy?

- **Backup**. You can suddenly [lose your data](https://www.nytimes.com/2022/08/21/technology/google-surveillance-toddler-photo.html) for reasons beyond your control. If you're a public person, you can be [de-platformed](https://en.wikipedia.org/wiki/Deplatforming). Before all this happens, it might be a good idea to back up most of your data.

- **Delete**. Backed up? Get rid of your digital footprint by exercising the [right to erasure](https://gdpr-info.eu/art-17-gdpr/).

- **Use** your data. Tech companies generally profit from using your data. Can it be of value to you? (Especially when combined from many sources) Here are some ideas.

  - *Study yourself*. See the [quantified self](https://quantifiedself.com/) and [personal science](https://leanpub.com/Personal-Science).
  - *Optimize apps for your productivity* (not ad sales). For example, build your [fair recommender system](https://arxiv.org/pdf/2105.12353.pdf).
  - *Contribute to science* by [participating](https://en.wikipedia.org/wiki/Citizen_science) in complex surveys involving your data (donate your data).

- **Audit** data privacy and protection compliance. Do they respect your rights? If something is not right, let's file a collective complaint.

## Usage

### Prepare data exports

Put your unzipped data exports in `exports` folder. 
You are going to have something like this:
```
exports/
    Takeout/
    Takeout 2/
    imap_export.mbox
```

### Install and run Apache Fuseki triplestore

Download Apache Fuseki release from the [Apache Jena Downloads](https://jena.apache.org/download/) page.

Extract and run the server:
```commandline
./fuseki-server
```

The triplestore will be available at http://localhost:3030.

Create a new dataset named 'mydata', it will be used to upload your data.

### Convert data exports into linked data

Install the python dependencies:
```commandline
pip install -r requirements.txt
```

Run the importer:
```commandline
python -m my_data.importer
```

Run the graph viewer:
```commandline
flask --app mydata.viewer:app run -h localhost -p 4999
```

Visit the viewer in your browser: http://localhost:4999
```