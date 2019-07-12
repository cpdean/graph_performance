# note, edge is a tag that regularly updates. the build script should be changed
# to pull every time
FROM redislabs/redisgraph:edge
RUN apt update \
    && apt install curl python3 -y \
    && curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py" \
    && python3 get-pip.py \
    && pip3 install redis
COPY ./performance.py /test_scripts/
COPY run_test.sh /test_scripts/
CMD ["/test_scripts/run_test.sh"]
