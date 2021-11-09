FROM fabric8/java-alpine-openjdk11-jre

RUN apk add --no-cache \
                bind-tools \
                iproute2 \
                nload

WORKDIR code
ADD docker/* ./
ADD log4j2.xml ./
ADD target/asdProj.jar ./
ADD babel_config.properties ./config.properties


#EXPOSE 10000/tcp

ENTRYPOINT ["./setupTc.sh"]
#CMD ["/bin/sh"]
#ADD tools/* ./