FROM python:3.10.13-bullseye

ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get install -y \
    vim \
    less \
    libxml2-dev \
    python \
    build-essential \
    make \
    gcc \
    python3-dev \
    locales \
    python3-pip \
    ruby-dev \
    rubygems \
    poppler-utils && \
    pip install --upgrade pip && \
    apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*

RUN mkdir /copo
WORKDIR /copo
COPY . /copo/
RUN pip install --use-deprecated=legacy-resolver -r /copo/requirement.txt

ENTRYPOINT ["bash","-c","python /copo/sample_audit.py"]

# wget -nv -O- https://download.calibre-ebook.com/linux-installer.sh | sh /dev/stdin &&
