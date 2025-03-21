#!/bin/bash

# Iniciar el servicio cron
service cron start

# Configurar logging de cron
touch /var/log/cron.log

printenv | grep -v "no_proxy" >> /etc/environment

tail -f /var/log/cron.log