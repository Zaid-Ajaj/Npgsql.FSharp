#!/bin/bash
pg_restore -U postgres -d dvdrental -F t /var/lib/postgresql/backup/dvdrental.tar