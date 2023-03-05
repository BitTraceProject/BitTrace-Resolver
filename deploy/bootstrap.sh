#!/bin/bash

DEPLOY_PWD=$PWD

cd "$DEPLOY_PWD"/.. || exit
bash build.sh
docker build . -t bittrace/resolver
echo "build container successfully!"

cd $DEPLOY_PWD/docker-compose || exit
cp $DEPLOY_PWD/docker-compose/.env.tmpl $DEPLOY_PWD/docker-compose/.env
docker-compose up -d
