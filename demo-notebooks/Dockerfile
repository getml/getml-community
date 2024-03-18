ARG TARGETPLATFORM
FROM --platform=${TARGETPLATFORM} python:3.11.8

RUN apt-get update \
    && apt-get install -y \
    locales \
    && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen

RUN useradd getml
USER getml
WORKDIR /home/getml

COPY --chown=getml:getml --chmod=0777 ./requirements.txt /home/getml/requirements.txt

ENV PATH="$PATH:/home/getml/.local/bin"
RUN python3.11 \
    -mpip install \
    -r /home/getml/requirements.txt

RUN mkdir /home/getml/.getML /home/getml/.getML/logs /home/getml/.getML/projects /home/getml/demo

EXPOSE 1709 8888
CMD [ "/home/getml/.local/bin/jupyter", "lab", "--ip='*'", "--port=8888", "--notebook-dir='/home/getml/demo'" ]