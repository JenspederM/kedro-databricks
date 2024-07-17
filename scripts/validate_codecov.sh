#/usr/bin/env bash

response=$(curl --data-binary @codecov.yml https://codecov.io/validate)

echo $response
if [[ "$response" =~ Error.* ]]; then
  echo
  echo "Codecov configuration is invalid"
  exit 1
fi
