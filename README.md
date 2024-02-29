# f1_stats_azure_databricks

# Project 25: WordSense Disambiguation 1

![Screenshot of the application](screenshots/7.png "Demo of WordSense Disambiguation using extended Lesk")

---


## Introduction


### Project Objectives:

1. **Objectives: 1** Comprehend and ensure the proper functioning of an existing WSD
   program that uses the Naive Bayes classifier for disambiguating word senses in the senseval-2 dataset.

2. **Enhancing Performance Metrics:** Extend the base program to include additional performance metrics such as
   accuracy, F1 score, precision, and recall.

3. **Classifier Comparison:** Implement and compare other classification algorithms like Random Forest, SVM, and
   Decision Trees, analyzing their performance in terms of F1 score, accuracy, precision, and recall.

4. **Preprocessing Influence:** Study the effect of different preprocessing steps on the performance metrics and present
   the findings.

5. **Feature Influence:** Test how different features (such as the number of features, tf-idf, and bi-grams) impact the
   performance of the classifiers.

6. **Lesk Algorithm Implementation and Testing:** Utilize NLTK's Lesk algorithm for WSD and compare its results against
   the Naive Bayes classifier on both training and testing datasets.

7. **Expanded Lesk Algorithm:** Design and implement an enhanced version of the Lesk algorithm that includes related
   terms extracted from WordNet, and compare its effectiveness with the standard Lesk algorithm.

8. **Results Comparison and Analysis:** Compare results on a sample of the Senseval2 dataset to draw conclusions on the
   effectiveness of the various methods employed.

9. **GUI Development:** Develop a simple graphical user interface (GUI) that enables users to input a target word and
   context for disambiguation, displaying results from both the simple and expanded Lesk algorithms.

## Project Folder Structure

Below is the outline of the project's directory and file structure:

```bash
wsd-senseval-lesk # Root folder of the project
│
├── app.py # The flask application entry point [task 8]
│
├── web # React web application (frontend) [task 8]
│
├── notebooks # Jupyter notebooks for all analysis and explorations
│ └── .ipynb files # Individual Jupyter notebooks for analysis [tasks 1-8]
│
├── scripts # Python scripts 
│ └── .py files # Standalone Python scripts for various tasks (mostly various implementation of lesk) [task 6-7]
│
├── data # Data directory for CSV exports from our analysis and datasets
│ └── .csv files # CSV dataset files
│
├── *.sh # Customized shell scripts for easily setting up and running this project
│
├── Dockerfile # For running this project as docker container [under development]
│
└── screenshots # Screenshots of the app for documentation
```


### Tools

To run this project, you'll need to install the following software:

- **Jupyter Notebook**: For interactive computing and sharing of live code, equations, visualizations, and narrative
  text. [Installation Guide](https://jupyter.org/install)

- **Node.js and npm**: Node.js is a JavaScript runtime built on Chrome's V8 JavaScript engine, and npm is the package
  manager for
  JavaScript. [Installation Guide for Node.js](https://nodejs.org/en/download/), [npm is included with Node.js](https://www.npmjs.com/get-npm)

- **Python 3**: An interpreted, high-level, general-purpose programming language. Make sure you have Python 3.10 or
  later for this project. [Installation Guide](https://www.python.org/downloads/)

- **Flask**: A lightweight WSGI web application framework in Python. It is designed to make getting started quick and
  easy, with the ability to scale up to complex
  applications. [Installation Guide](https://flask.palletsprojects.com/en/latest/installation/)

- **NLTK**: A leading platform for building Python programs to work with human language
  data. [Installation Guide](https://www.nltk.org/install.html)

- **Docker**: Docker is a set of platform as a service products that use OS-level virtualization to deliver software in
  packages called containers. [Installation Guide](https://docs.docker.com/get-docker/)

Please follow the installation guides provided by the links to ensure that your environment is set up correctly. After
installation, you can verify the installation of each by checking their versions using the respective command line
tools.

For example, to check if you have the correct version of Python installed, run:

```sh
python3 --version
```

```bash
node --version
npm --version
```

Once all dependencies are installed, you can proceed with setting up the virtual environment and running the project as
described below.


## Important Notes

- Before running any of the scripts, ensure that you have Python 3.10 and npm installed on your machine.
- You may need to activate the virtual environment created by `setup_venv.sh` script manually
  using `source venv/bin/activate` (for Unix-like OS) or `.\venv\Scripts\activate` (for Windows) to run Python
  applications within the virtual environment.
- The scripts assume the presence of `requirements.txt` and `package.json` in their respective directories for
  dependency management.
- Ensure you are in the root directory of the project when running these scripts.

If you encounter any permissions issues, you may need to run the scripts with `sudo` on Unix-like systems or as an
administrator on Windows.

[![View Online Demo](https://img.shields.io/badge/View-Online%20Demo-blue?style=for-the-badge)](http://brhnme.pythonanywhere.com/)
