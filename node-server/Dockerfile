FROM node:14.14

WORKDIR /home
ADD ./package.json .
RUN npm install

ADD . .

EXPOSE 8001

ENTRYPOINT [ "node", "--experimental-json-modules", "app.js" ]
