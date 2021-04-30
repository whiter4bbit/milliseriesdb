FROM ubuntu

COPY target/release/milliseriesdb /milliseriesdb

RUN chmod +x /milliseriesdb

EXPOSE 8080

ENTRYPOINT [ "./milliseriesdb" ]