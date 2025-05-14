# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

FROM python:3.8.18

RUN apt-get update \
    && apt-get install -y \
    locales \
    && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen

RUN useradd getml
USER getml
WORKDIR /home/getml

ENV PATH="/home/getml/.local/bin:${PATH}"

RUN pip install --user --upgrade pip

COPY requirements.for-pystan.txt requirements.for-fbprophet.txt requirements.for-kats.txt requirements.txt ./
RUN pip install --user -r requirements.for-pystan.txt \
    && pip install --user -r requirements.for-fbprophet.txt \
    && pip install --user -r requirements.for-kats.txt \
    && pip install --user -r requirements.txt
 
ARG VERSION_NUMBER
ARG GETML_VERSION

WORKDIR /home/getml
RUN mkdir /home/getml/.getML

RUN mkdir engine | curl https://static.getml.com/download/$VERSION_NUMBER/$GETML_VERSION.tar.gz | tar -xz --strip-components=1 -C engine -f - ${GETML_VERSION}
RUN curl https://static.getml.com/download/$VERSION_NUMBER/python.tar.gz | tar -xzf -
WORKDIR /home/getml/python
RUN pip3 install .

COPY --chown=getml:getml . /home/getml/benchmarks/
WORKDIR /home/getml/benchmarks
CMD [ "/home/getml/benchmarks/run_benchmarks.sh" ]