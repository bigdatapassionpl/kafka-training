#!/bin/bash

mvn deploy:deploy-file \
  "-DgroupId=com.bigdatapassion" \
  "-DartifactId=kafka-training" \
  "-Dversion=1.0-SNAPSHOT" \
  "-Dpackaging=jar" \
  "-Dfile=pom.xml" \
  "-DrepositoryId=radek-maven-repo-snapshot" \
  "-Durl=s3://radek-maven-repo/snapshot"
