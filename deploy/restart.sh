#!/bin/bash

DEPLOY_PWD=$PWD
OUTPUT_DIR=$PWD/../output

function restart() {
  set -x
  cd $DEPLOY_PWD/.. || exit
  bash $DEPLOY_PWD/../build.sh

  # cp 会直接覆盖旧的
  echo "rebuild and restart:[mgr.resolver.bittrace.proj]"
  docker cp ${OUTPUT_DIR}/receiver-cli "mgr.resolver.bittrace.proj":/bittrace/
  docker restart "mgr.resolver.bittrace.proj"

  cd $DEPLOY_PWD || exit
}

restart
