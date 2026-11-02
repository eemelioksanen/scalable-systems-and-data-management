FROM docker.io/library/python@sha256:16bf2b5c59a08523519d3c8589deca849285dee1d62a3653e124c3dba340f15e
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    latexmk texlive-latex-extra texlive-publishers texlive-fonts-recommended \
    texlive-science texlive-fonts-extra texlive-plain-generic poppler-utils \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /work
