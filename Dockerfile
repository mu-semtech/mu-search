FROM semtech/mu-jruby-template

LABEL maintainer="redpencil <info@redpencil.io>"
# 200MB
ENV MAXIMUM_FILE_SIZE="209715200"
# seconds
ENV ELASTIC_READ_TIMEOUT="180"
ENV LOG_LEVEL="info"
ENV LOG_SCOPE_SETUP="info"
ENV LOG_SCOPE_INDEX_MGMT="info"
ENV LOG_SCOPE_TIKA="warn"
ENV LOG_SCOPE_AUTHORIZATION="warn"
ENV LOG_SCOPE_DELTA="info"
ENV LOG_SCOPE_UPDATE_HANDLER="info"
ENV LOG_SCOPE_INDEXING="info"
ENV LOG_SCOPE_SEARCH="warn"
ENV LOG_SCOPE_SPARQL="warn"
ENV LOG_SCOPE_ELASTICSEARCH="error"
