from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from parameterized_flows import etl_parent_flow

docker_container_block = DockerContainer.load("zoom")
docker_dep = Deployment.build_from_flow(flow=etl_parent_flow,name='docker-flow',infrastructure=docker_container_block)

if __name__ == "__main__":
    docker_dep.apply()
