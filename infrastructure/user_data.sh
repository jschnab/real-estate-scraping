#!/bin/bash

set -ex
set -o pipefail

exec > >(tee /var/log/user_data.log | logger -t user_data) 2>&1
echo BEGIN
BEGINTIME=$(date +%s)
date "+%Y-%m-%d %H:%M:%S"

yum update -y
amazon-linux-extras install -y epel
yum-config-manager --enable epel*
yum install -y git python3 gcc postgresql-devel python3-devel.x86_64 tor

pip3 install bs4 torrequest awscli boto3

TOR_PASSWORD=ind*Ica69

cat << EOT > /etc/tor/torrc
SOCKSPort 9050
ControlPort 9051
CookieAuthentication 1
EOT

echo "export TOR_PASSWORD=$TOR_PASSWORD" >> /home/ec2-user/.bashrc
echo "HashedControlPassword $(tor --hash-password "$TOR_PASSWORD" | grep --color=never 16:[A-Z0-9])" >> /etc/tor/torrc
systemctl restart tor

git clone https://github.com/jschnab/real-estate-scraping.git /home/ec2-user/real-estate-scraping

mkdir /home/ec2-user/.aws
cat << EOT > /home/ec2-user/.aws
[default]
region = us-east-1
output = json
EOT

aws s3 cp --recursive s3://jschnab-test-bucket/rea-estate/browser_config /home/ec2-user/.browsing

chown -R ec2-user:ec2-user /home/ec2-user/

date "+%Y-%m-%d %H:%M:%S"
ENDTIME=$(date +%s)
echo "deployment took $((ENDTIME - BEGINTIME)) seconds"
echo END
