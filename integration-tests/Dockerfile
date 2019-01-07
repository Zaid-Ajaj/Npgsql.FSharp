FROM postgres:latest
ENV POSTGRES_DB dvdrental
COPY dvdrental.tar /var/lib/postgresql/backup/dvdrental.tar
COPY restore_dvdrental.sh /docker-entrypoint-initdb.d/restore_dvdrental.sh