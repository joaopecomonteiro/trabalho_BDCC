import concurrent.futures
from unittest import result

import flask
from flask import Flask, jsonify, render_template, request
from google.cloud import bigquery, storage

from datetime import datetime

app = flask.Flask(__name__)
bigquery_client = bigquery.Client()

@app.route('/')
def index():
    # TODO

    stats = {}

    query_job = bigquery_client.query(
            """
            SELECT COUNT(*) AS patients 
            FROM `bdcc-451416.Dataset1.PATIENTS`
            """
        )

    results = query_job.result()

    # print(query_job.result())

    data = [row.patients for row in results]

    stats['patients'] = data[0]

    return render_template('index.html', stats=stats)



@app.route('/patients/')
def list_patients():
    query_job = bigquery_client.query(
            """
            SELECT
                SUBJECT_ID,
                GENDER,
                DOB
            FROM `bdcc-451416.Dataset1.PATIENTS`
            ORDER BY SUBJECT_ID ASC
            LIMIT 20
            """
        )

    results = query_job.result()

    data = [
        {"SUBJECT_ID": row.SUBJECT_ID, "GENDER": row.GENDER, "DOB": row.DOB}
        for row in results
    ]
    return render_template('patients-list.html', data=data)


@app.route('/patients/<int:id>/')
def get_patient(id):

    query = """
          SELECT *
          FROM `bdcc-451416.Dataset1.PATIENTS`
          WHERE SUBJECT_ID = @subject_id
          """

    # Define the job configuration and pass parameters safely
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("subject_id", "INT64", id)
        ]
    )

    query_job = bigquery_client.query(query, job_config=job_config)
    results = query_job.result()

    data = [
        {"SUBJECT_ID": row.SUBJECT_ID, "GENDER": row.GENDER, "DOB": row.DOB, "DOD": row.DOD,
         "DOD_HOSP": row.DOD_HOSP, "DOD_SSN": row.DOD_SSN, "EXPIRE_FLAG": row.EXPIRE_FLAG,
         "PHOTO_URL": f"{row.PHOTO_URL}" if row.PHOTO_URL else None}
        for row in results
    ]


    if len(data) == 0:
        return render_template('patient-not-exist.html',
                               ID=id)

    return render_template('patient.html',
           patient=data[0])



@app.route('/admissions/')
def list_admissions():
    query_job = bigquery_client.query(
            """
            SELECT
                SUBJECT_ID,
                HADM_ID,
                ADMITTIME,
                DISCHTIME,
                DIAGNOSIS,
                HOSPITAL_EXPIRE_FLAG
                FROM `bdcc-451416.Dataset1.ADMISSIONS`
            ORDER BY HADM_ID ASC
            LIMIT 20
            """
        )

    results = query_job.result()

    data = [
        {"SUBJECT_ID": row.SUBJECT_ID, "HADM_ID": row.HADM_ID, "ADMITTIME": row.ADMITTIME,
         "DISCHTIME": row.DISCHTIME, "DIAGNOSIS": row.DIAGNOSIS, "HOSPITAL_EXPIRE_FLAG": row.HOSPITAL_EXPIRE_FLAG}
        for row in results
    ]
    return render_template('admissions-list.html', data=data)



@app.route('/admissions/<int:id>/')
def get_admission(id):
    query = """
          SELECT *
          FROM `bdcc-451416.Dataset1.ADMISSIONS`
          WHERE HADM_ID = @hadm_id
          """

    # Define the job configuration and pass parameters safely
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("hadm_id", "INT64", id)
        ]
    )

    query_job = bigquery_client.query(query, job_config=job_config)
    results = query_job.result()

    data = [
        {"HADM_ID": row.HADM_ID, "SUBJECT_ID": row.SUBJECT_ID, "ADMITTIME": row.ADMITTIME, "DISCHTIME": row.DISCHTIME,
         "DEATHTIME": row.DEATHTIME, "ADMISSION_TYPE": row.ADMISSION_TYPE, "ADMISSION_LOCATION": row.ADMISSION_LOCATION,
         "DISCHARGE_LOCATION": row.DISCHARGE_LOCATION, "LANGUAGE": row.LANGUAGE, "INSURANCE": row.INSURANCE,
         "ETHNICITY": row.ETHNICITY, "MARITAL_STATUS": row.MARITAL_STATUS, "RELIGION": row.RELIGION,
         "DIAGNOSIS": row.DIAGNOSIS, "EDOUTTIME": row.EDOUTTIME, "EDREGTIME": row.EDREGTIME,
         "HOSPITAL_EXPIRE_FLAG": row.HOSPITAL_EXPIRE_FLAG, "HAS_CHARTEVENTS_DATA": row.HAS_CHARTEVENTS_DATA}
        for row in results
    ]

    if len(data) == 0:
        return render_template('admission-not-exist.html',
                               ID=id)

    return render_template('admission.html',
           admission=data[0])



@app.route('/labevent/<int:id>/')
def get_lab_event(id):

    query = """
          SELECT *
          FROM `bdcc-451416.Dataset1.LABEVENTS`
          WHERE SUBJECT_ID = @subject_id
          ORDER BY ITEMID, CHARTTIME ASC
          """

    # Define the job configuration and pass parameters safely
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("subject_id", "INT64", id)
        ]
    )

    query_job = bigquery_client.query(query, job_config=job_config)
    results = query_job.result()

    data = [
        {"SUBJECT_ID": row.SUBJECT_ID, "HADM_ID": row.HADM_ID, "ITEMID": row.ITEMID, "CHARTTIME": row.CHARTTIME,
         "VALUE": row.VALUE, "VALUENUM": row.VALUENUM, "VALUEUOM": row.VALUEUOM, "FLAG": row.FLAG}
        for row in results
    ]

    if len(data) == 0:
        return render_template('labevent-not-exist.html',
                               ID=id)

    return render_template('labevent.html',
           data=data)


@app.route('/inputevent/<int:id>/')
def get_input_event(id):

    query = """
          SELECT *
          FROM `bdcc-451416.Dataset1.INPUTEVENTS`
          WHERE SUBJECT_ID = @subject_id
          ORDER BY ITEMID, STARTTIME ASC
          """

    # Define the job configuration and pass parameters safely
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("subject_id", "INT64", id)
        ]
    )

    query_job = bigquery_client.query(query, job_config=job_config)
    results = query_job.result()

    data = [
        {
            "SUBJECT_ID": row.SUBJECT_ID,
            "HADM_ID": row.HADM_ID,
            "ICUSTAY_ID": row.ICUSTAY_ID,
            "STARTTIME": row.STARTTIME,
            "ENDTIME": row.ENDTIME,
            "ITEMID": row.ITEMID,
            "AMOUNT": row.AMOUNT,
            "AMOUNTUOM": row.AMOUNTUOM,
            "RATE": row.RATE,
            "RATEUOM": row.RATEUOM,
            "STORETIME": row.STORETIME,
            "CGID": row.CGID,
            "ORDERID": row.ORDERID,
            "LINKORDERID": row.LINKORDERID,
            "ORDERCATEGORYNAME": row.ORDERCATEGORYNAME,
            "SECONDARYORDERCATEGORYNAME": row.SECONDARYORDERCATEGORYNAME,
            "ORDERCOMPONENTTYPEDESCRIPTION": row.ORDERCOMPONENTTYPEDESCRIPTION,
            "ORDERCATEGORYDESCRIPTION": row.ORDERCATEGORYDESCRIPTION,
            "PATIENTWEIGHT": row.PATIENTWEIGHT,
            "TOTALAMOUNT": row.TOTALAMOUNT,
            "TOTALAMOUNTUOM": row.TOTALAMOUNTUOM,
            "ISOPENBAG": row.ISOPENBAG,
            "CONTINUEINNEXTDEPT": row.CONTINUEINNEXTDEPT,
            "CANCELREASON": row.CANCELREASON,
            "STATUSDESCRIPTION": row.STATUSDESCRIPTION,
            "COMMENTS_EDITEDBY": row.COMMENTS_EDITEDBY,
            "COMMENTS_CANCELEDBY": row.COMMENTS_CANCELEDBY,
            "COMMENTS_DATE": row.COMMENTS_DATE,
            "ORIGINALAMOUNT": row.ORIGINALAMOUNT,
            "ORIGINALRATE": row.ORIGINALRATE
        }
        for row in results
    ]

    if len(data) == 0:
        return render_template('inputevent-not-exist.html',
                               ID=id)

    return render_template('inputevent.html',
           data=data)


@app.route('/patients/delete/<int:id>/')
def delete_patient(id):

    query = """
          DELETE FROM `bdcc-451416.Dataset1.PATIENTS`
          WHERE SUBJECT_ID = @subject_id
          """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("subject_id", "INT64", id)
        ]
    )

    query_job = bigquery_client.query(query, job_config=job_config)




    return render_template('patient-deleted.html',
           ID=id)


@app.route('/patients/create/')
def create_patient():
    return render_template('patient-create.html')



@app.route('/patients/created', methods=['GET', 'POST'])
def patient_created():
    if request.method == 'POST':

        query = f"""
        SELECT SUBJECT_ID FROM `bdcc-451416.Dataset1.PATIENTS`
        ORDER BY SUBJECT_ID DESC
        LIMIT 1
        """

        query_job = bigquery_client.query(query)
        results = query_job.result()

        for row in results:
            SUBJECT_ID = row.SUBJECT_ID

        SUBJECT_ID = SUBJECT_ID+1

        GENDER = request.form['GENDER']


        # Check if the user provided a DOB
        DOB_input = request.form['DOB']  # From HTML form
        if DOB_input:  # If user provided a value
            DOB = datetime.strptime(DOB_input, "%Y-%m-%dT%H:%M")  # Convert to datetime
            DOB = DOB.strftime("%Y-%m-%d %H:%M:%S+00:00")  # Convert to BigQuery TIMESTAMP format
        else:
            DOB = None  # Set as None if empty

        # Check if the user provided a DOB
        DOD_input = request.form['DOD']  # From HTML form
        if DOD_input:  # If user provided a value
            DOD = datetime.strptime(DOD_input, "%Y-%m-%dT%H:%M")  # Convert to datetime
            DOD = DOD.strftime("%Y-%m-%d %H:%M:%S+00:00")  # Convert to BigQuery TIMESTAMP format
        else:
            DOD = None  # Set as None if empty



        # Check if the user provided a DOB
        DOD_HOSP_input = request.form['DOD_HOSP']  # From HTML form
        if DOD_HOSP_input:  # If user provided a value
            DOD_HOSP = datetime.strptime(DOD_HOSP_input, "%Y-%m-%dT%H:%M")  # Convert to datetime
            DOD_HOSP = DOD_HOSP.strftime("%Y-%m-%d %H:%M:%S+00:00")  # Convert to BigQuery TIMESTAMP format
        else:
            DOD_HOSP = None  # Set as None if empty



        # Check if the user provided a DOB
        DOD_SSN_input = request.form['DOD_SSN']  # From HTML form
        if DOD_SSN_input:  # If user provided a value
            DOD_SSN = datetime.strptime(DOD_SSN_input, "%Y-%m-%dT%H:%M")  # Convert to datetime
            DOD_SSN = DOD_SSN.strftime("%Y-%m-%d %H:%M:%S+00:00")  # Convert to BigQuery TIMESTAMP format
        else:
            DOD_SSN = None  # Set as None if empty

        EXPIRE_FLAG = int(request.form['EXPIRE_FLAG'])


        # BigQuery SQL to INSERT a new row with parameters
        query = f"""
        INSERT INTO `bdcc-451416.Dataset1.PATIENTS` (SUBJECT_ID, GENDER, DOB, DOD, DOD_HOSP, DOD_SSN, EXPIRE_FLAG)
        VALUES (@SUBJECT_ID, @GENDER, @DOB, @DOD, @DOD_HOSP, @DOD_SSN, @EXPIRE_FLAG)
        """

        query_parameters = [
                bigquery.ScalarQueryParameter("SUBJECT_ID", "INT64", SUBJECT_ID),
                bigquery.ScalarQueryParameter("GENDER", "STRING", GENDER),
                bigquery.ScalarQueryParameter("DOB", "TIMESTAMP", DOB),
                bigquery.ScalarQueryParameter("DOD", "TIMESTAMP", DOD),
                bigquery.ScalarQueryParameter("DOD_HOSP", "TIMESTAMP", DOD_HOSP),
                bigquery.ScalarQueryParameter("DOD_SSN", "TIMESTAMP", DOD_SSN),
                bigquery.ScalarQueryParameter("EXPIRE_FLAG", "INT64", EXPIRE_FLAG)
            ]


        # Define the job configuration and pass parameters safely
        job_config = bigquery.QueryJobConfig(
            query_parameters=query_parameters
        )

        # Run the query
        query_job = bigquery_client.query(query, job_config=job_config)
        query_job.result()  # Wait for job to complete



    return render_template('patient-created.html', ID=SUBJECT_ID)


# def allowed_file(filename):
#     """Check if the uploaded file has an allowed extension."""
#     return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
#


import os
# from werkzeug.utils import secure_filename


# UPLOAD_FOLDER = 'static/uploads/'
# ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}
# app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

BUCKET_NAME = "bdcc-451416.appspot.com"


def upload_photo_to_gcs(file, subject_id):
    """Uploads a photo to Google Cloud Storage and returns the URL."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # File name in GCS
    blob = bucket.blob(f"patients/{subject_id}.png")

    # Upload file
    blob.upload_from_file(file, content_type=file.content_type)

    blob.make_public()

    # Return the public URL (or private URL if access control is needed)
    return f"https://storage.googleapis.com/{BUCKET_NAME}/patients/{subject_id}.png"


def update_bigquery_photo_url(subject_id, photo_url):
    """Updates the patient's photo URL in BigQuery."""
    bigquery_client = bigquery.Client()
    query = f"""
    UPDATE `bdcc-451416.Dataset1.PATIENTS`
    SET PHOTO_URL = '{photo_url}'
    WHERE SUBJECT_ID = {subject_id}
    """
    bigquery_client.query(query).result()



@app.route('/patients/<int:id>/upload_photo', methods=['POST'])
def upload_patient_photo(id):
    """Handles photo upload and updates BigQuery with the photo URL."""
    if 'photo' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['photo']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    # Upload photo to Google Cloud Storage
    photo_url = upload_photo_to_gcs(file, id)

    # Update BigQuery record with the photo URL
    update_bigquery_photo_url(id, photo_url)

    return get_patient(id)

    # return render_template('photo-uploaded.html', ID=id)

















def patient_exists(subject_id):
    query = """
          SELECT SUBJECT_ID
          FROM `bdcc-451416.Dataset1.PATIENTS`
          WHERE SUBJECT_ID = @subject_id
          """

    # Define the job configuration and pass parameters safely
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("subject_id", "INT64", subject_id)
        ]
    )

    query_job = bigquery_client.query(query, job_config=job_config)
    results = query_job.result()

    data = [
        {"SUBJECT_ID": row.SUBJECT_ID}
        for row in results
    ]

    return len(data) > 0


@app.route('/make-question/<int:id>/')
def make_question(id):
    query = """
          SELECT *
          FROM `bdcc-451416.Dataset1.DEFAULT_QUESTIONS`
          """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("subject_id", "INT64", id)
        ]
    )

    query_job = bigquery_client.query(query, job_config=job_config)
    results = query_job.result()

    data = [
        {"QUESTION_ID": row.QUESTION_ID, "QUESTION": row.QUESTION}
        for row in results
    ]

    if not patient_exists(id):
        return render_template('patient-not-exist.html',
                               ID=id)


    return render_template('make-question.html',
           data=data, ID=id)




@app.route('/submit-question/<int:id>/', methods=["GET", "POST"])
def submit_question(id):
    question = request.form.get("question", "").strip()
    custom_question = request.form.get("custom_question", "").strip()

    # If "Other" is selected, use the custom question
    if question == "custom":
        question = custom_question

        return render_template('answer-question.html', ID=id, question=question)

    else:
        if question == "What care unit is the patient in?":
            query= """
                SELECT FIRST_CAREUNIT
                FROM `bdcc-451416.Dataset1.ICUSTAYS`
                WHERE SUBJECT_ID = @subject_id
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("subject_id", "INT64", id)
                ]
            )

            query_job = bigquery_client.query(query, job_config=job_config)
            results = query_job.result()


            answer = [row.FIRST_CAREUNIT for row in results][0]

        elif question == "When did the patient arrive?":
            query = """
                        SELECT ADMITTIME
                        FROM `bdcc-451416.Dataset1.ADMISSIONS`
                        WHERE SUBJECT_ID = @subject_id
                    """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("subject_id", "INT64", id)
                ]
            )

            query_job = bigquery_client.query(query, job_config=job_config)
            results = query_job.result()

            answer = str([row.ADMITTIME for row in results][0])

        elif question == "Is he alive?":
            query = """
                        SELECT EXPIRE_FLAG
                        FROM `bdcc-451416.Dataset1.PATIENTS`
                        WHERE SUBJECT_ID = @subject_id
                    """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("subject_id", "INT64", id)
                ]
            )

            query_job = bigquery_client.query(query, job_config=job_config)
            results = query_job.result()

            answer = [row.EXPIRE_FLAG for row in results][0]
            answer = "Dead" if answer==1 else "Alive"



        query = f"""
         INSERT INTO `bdcc-451416.Dataset1.QUESTIONS` (SUBJECT_ID, QUESTION, ANSWER)
         VALUES (@SUBJECT_ID, @QUESTION, @ANSWER)
         """

        query_parameters = [
            bigquery.ScalarQueryParameter("SUBJECT_ID", "INT64", id),
            bigquery.ScalarQueryParameter("QUESTION", "STRING", question),
            bigquery.ScalarQueryParameter("ANSWER", "STRING", answer)
        ]

        # Define the job configuration and pass parameters safely
        job_config = bigquery.QueryJobConfig(
            query_parameters=query_parameters
        )

        # Run the query
        query_job = bigquery_client.query(query, job_config=job_config)
        query_job.result()  # Wait for job to complete

    return render_template('submit-question.html', ID=id, question=question, answer=answer)



@app.route('/answer-question/<int:id>/', methods=["GET", "POST"])
def question_answered(id):
    answer = request.form.get('answer')
    question = request.form.get('question')


    query = f"""
             INSERT INTO `bdcc-451416.Dataset1.QUESTIONS` (SUBJECT_ID, QUESTION, ANSWER)
             VALUES (@SUBJECT_ID, @QUESTION, @ANSWER)
             """

    query_parameters = [
        bigquery.ScalarQueryParameter("SUBJECT_ID", "INT64", id),
        bigquery.ScalarQueryParameter("QUESTION", "STRING", question),
        bigquery.ScalarQueryParameter("ANSWER", "STRING", answer)
    ]

    # Define the job configuration and pass parameters safely
    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )

    # Run the query
    query_job = bigquery_client.query(query, job_config=job_config)
    query_job.result()  # Wait for job to complete

    return render_template('submit-question.html', ID=id, question=question, answer=answer)















from google.cloud import logging
import functions_framework


logging_client = logging.Client()
logger = logging_client.logger("recurrent-function")



@functions_framework.http
def recurrent_task(request):
    """A Google Cloud Function that returns a simple message on a webpage."""

    html_response = """
    <html>
    <head>
        <title>Recurrent Task</title>
    </head>
    <body>
        <h1>Recurrent Task Executed</h1>
        <p>This function runs on a schedule.</p>
    </body>
    </html>
    """

    return html_response, 200, {'Content-Type': 'text/html'}












