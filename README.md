1. Build docker image: `docker build . -t jowookjae.in:5000/deepquant-server:VERSION --platform linux/amd64`
2. Push docker image: `docker push jowookjae.in:5000/deepquant-server:VERSION`
3. Run container in remote: `docker run -d -p 8888:8080 jowookjae.in:5000/deepquant-server:VERSION`

## Like this
```
docker build . -t jowookjae.in:5000/deepquant-server:latest --platform linux/amd64
docker push jowookjae.in:5000/deepquant-server:latest
docker run -d -p 8888:8080 jowookjae.in:5000/deepquant-server:latest
```