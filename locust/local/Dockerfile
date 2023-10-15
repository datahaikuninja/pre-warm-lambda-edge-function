FROM locustio/locust
COPY . /mnt/locust/
ENTRYPOINT ["/bin/sh", "-c", "locust -f /mnt/locust/locustfile.py --host=$TARGET_HOST --headless -u $USERS -r $SPAWN_RATE --run-time $RUN_TIME"]
