#!/bin/bash

version=$(cat README.md | grep version | cut -d: -f 2)
version=${version:1} # delete leading space
tag="enocmartinez/sensorthings-timeseries:${version}-ubuntu"
echo "Building image with tag: $tag"
docker build -t ${tag} .
docker push "${tag}"