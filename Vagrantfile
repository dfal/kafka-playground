# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

	config.vm.box = "ubuntu/trusty64"

	config.vm.provider "virtualbox" do |v|
		v.memory = 2048
	end

	config.vm.network :forwarded_port, guest: 2181, host: 2181, auto_correct: true

	config.vm.network "private_network", ip: "192.168.50.5"

	config.vm.provision :docker
	config.vm.provision :docker_compose, rebuild: true, run: "always", yml: "/vagrant/docker-compose.yml"

end
