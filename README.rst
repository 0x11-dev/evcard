
package:
python -m zipapp evcard

scp evcard.pyz evcard@10.20.140.1:apps

run:
python evcard.pyz ...


for i in {1..12}; do
dt=$(printf "2014%02d" $i)
python evcard.pyz $dt
if [[ $? -ne 0 ]]; then
break
fi
redis-cli flushdb
done



vehicle:created_time，register_date
vehicle_offline_info: off_time, recover_time
shop_info: first_online_time
order_info: created_time，
assess_info：created_time
membership_info: reg_time, review_time




INSTALL

vim /etc/apt/source.list:
  deb [arch=amd64] http://archive.cloudera.com/kudu/ubuntu/trusty/amd64/kudu trusty-kudu5 contrib
  deb-src http://archive.cloudera.com/kudu/ubuntu/trusty/amd64/kudu trusty-kudu5 contrib
$>
  sudo apt-get install libkuduclient0
  sudo apt-get install libkuduclient-dev
# switch gcc to 4.9.x
$>
  pip install --no-cache-dir kudu-python

