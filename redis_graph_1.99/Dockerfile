FROM redislabs/redisgraph:1.99.0
RUN apt update \
    && apt install curl python3 -y \
    && curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py" \
    && python3 get-pip.py \
    && pip3 install redis
COPY ./performance.py /test_scripts/
COPY run_test.sh /test_scripts/
CMD ["/test_scripts/run_test.sh"]
