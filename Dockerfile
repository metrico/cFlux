# nonFlux Clickhouse
FROM node:8

# BUILD FORCE
ENV BUILD 703021

# Requires HOMER7-UI Git
# RUN git clone https://github.com/lmangani/nonFlux /app
COPY . /app
WORKDIR /app
RUN npm install 

# Expose Ports
EXPOSE 8686

CMD [ "npm", "start" ]
