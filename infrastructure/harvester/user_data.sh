#!/bin/bash

set -ex
set -o pipefail

exec > >(tee /var/log/user_data.log | logger -t user_data) 2>&1
echo BEGIN
BEGINTIME=$(date +%s)
date "+%Y-%m-%d %H:%M:%S"

TOR_PASSWORD=
S3_BUCKET=

yum update -y
amazon-linux-extras install -y epel
yum-config-manager --enable epel*
yum install -y git python3 gcc postgresql-devel python3-devel.x86_64 tor

pip3 install bs4 torrequest awscli boto3

cat << EOT > /etc/tor/torrc
SOCKSPort 9050
ControlPort 9051
CookieAuthentication 1
EOT

mkdir /home/ec2-user/.browsing
echo "TOR_PASSWORD=$TOR_PASSWORD" > /home/ec2-user/.browsing/vars.conf
echo "HashedControlPassword $(tor --hash-password "$TOR_PASSWORD" | grep --color=never 16:[A-Z0-9])" >> /etc/tor/torrc
systemctl restart tor

mkdir /home/ec2-user/.aws
cat << EOT > /home/ec2-user/.aws/config
[default]
region = us-east-1
output = json
EOT

git clone https://github.com/jschnab/real-estate-scraping.git /home/ec2-user/real-estate-scraping
aws s3 cp --recursive s3://"$S3_BUCKET"/real-estate/browser_config /home/ec2-user/.browsing

chown -R ec2-user:ec2-user /home/ec2-user/

cat << EOF > /etc/systemd/system/harvest_re.service
[Unit]
Description=Automated web browser for real estate
After=network.target

[Service]
Type=simple
EnvironmentFile=/home/ec2-user/.browsing/vars.conf
User=ec2-user
Group=ec2-user
ExecStart=/usr/bin/python3 /home/ec2-user/real-estate-scraping/test.py harvest
Restart=always
TimeoutStartSec=10
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
EOF

systemctl enable harvest_re.service
systemctl start harvest_re.service

date "+%Y-%m-%d %H:%M:%S"
ENDTIME=$(date +%s)
echo "deployment took $((ENDTIME - BEGINTIME)) seconds"
echo END