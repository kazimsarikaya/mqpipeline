#!/bin/sh

tox && cd tests && \
    podman compose up --build --force-recreate --remove-orphans
