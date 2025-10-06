# Metis web resource id finder repository
## Table of Contents
- [Table of Contents](#table-of-contents)
- [About the Project](#about-the-project)
- [Description](#description)
- [Built With](#built-with)
- [Getting Started](#getting-started)
- [Prerequisites](#prerequisites-and-dependencies)
- [Installation](#installation-of-library)
- [Usage](#usage)
- [Example](#example)
- [Architecture](#design-architecture)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [Acknowledgements](#acknowledgements)

# About the Project

# Description
Rest API to find the hashcode of a webresource 

# Built With
* [Java](https://www.java.com)
* [Junit](https://junit.org/junit5/)
* [Spring Boot](https://spring.io)

# Getting Started
# Prerequisites and Dependencies

# Installation of library
### Get the repository
```shell
git clone https://github.com/europeana/metis-tools.git
```
this is part of metis tools and use the folder `metis-webresource-finder` to use it.

# Usage
#### Enable API Endpoint 
### 1. Hash code  is possible with the command:
```shell
curl -X 'GET' \
  'http://localhost:8080/webresource/hashcode?webresourceId={webresourceId}&recordId={recordId}' \
  -H 'accept: application/json'
```
where 
```
{webresourceId} is the url of the resource
{recordId} is the id of the record usually rdf:about
```

Response:
```
HTTP code: 200 
Content-Type: application/json
{
  "webResourceId": "web resource",
  "recordId": "record id",
  "hashCodeId": "9f27a9e17621f49a1983d7992427a0c1"
}
``` 
# Example
to run spring boot application from command line
```
mvn spring-boot:run
```
uses swagger default configuration `http://localhost:8080/swagger-ui/index.html`

# Roadmap
- [x] initial release.
- [ ] ...

# Contributing
Any contributions you make are appreciated.

1. Fork the Project
2. Create your Feature Branch (git checkout -b feature/AmazingFeature)
3. Commit your Changes (git commit -m 'Add some AmazingFeature')
4. Push to the Branch (git push origin feature/AmazingFeature)
5. Open a Pull Request
