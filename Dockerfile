# nonFlux Clickhouse
FROM node:16-alpine

# BUILD FORCE
ENV BUILD 703022

# Requires HOMER7-UI Git
# RUN git clone https://github.com/lmangani/nonFlux /app
COPY . /app
WORKDIR /app
RUN npm install

# Expose Ports
EXPOSE 8686

CMD [ "npm", "start" ]
