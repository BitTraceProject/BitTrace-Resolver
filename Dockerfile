FROM ubuntu:20.04

WORKDIR /bittrace

COPY ./output/resolver-cli /bittrace/

VOLUME ["/root/.bittrace"]

# for resolver
EXPOSE 8083
# for pprof
EXPOSE 9093

ENTRYPOINT ["/bittrace/resolver-cli"]
