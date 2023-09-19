# noinspection SpellCheckingInspection
__author__ = 'wookjae.jo'

import subprocess

from about import version

DOCKER_REGISTRY_ADDR = 'jowookjae.in:5000'
APP_NAME = "custardchip-api-server"
IMAGE_NAME = f'{DOCKER_REGISTRY_ADDR}/{APP_NAME}:{version}'


def build():
    """
    Build the application as docker image
    """
    subprocess.check_call(f'docker build . -t {IMAGE_NAME} --platform linux/amd64',
                          shell=True)


def push():
    """
    Push the docker image to the registry
    """
    subprocess.check_call(f'docker push {IMAGE_NAME}', shell=True)


def main():
    build()
    push()

    print('=' * 80)
    print('Execute the command below to run the container in the target host:')
    options = f"-d --restart=unless-stopped -p 8888:8080 --name {APP_NAME} --restart unless-stopped"
    print(f"docker run {options} {IMAGE_NAME}")
    print('=' * 80)


if __name__ == '__main__':
    main()
