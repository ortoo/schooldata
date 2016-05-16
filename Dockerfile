FROM       quay.io/ortoo/chore-base

# Install python dependencies
RUN        pip3 install loggly-handler gcloud


# Add the files and set ownership
ADD        . /home/chore/school-data
RUN        chown -R chore /home/chore

WORKDIR    /home/chore/school-data

CMD        python3 update_school_data.py
