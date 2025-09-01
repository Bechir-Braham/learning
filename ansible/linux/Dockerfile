
# Use the official image as a parent image
FROM ubuntu

# Update the system, install OpenSSH Server, and set up users
RUN apt-get update && apt-get install -y openssh-server

# Create user and set password for user and root user
RUN  useradd -rm -d /home/bb -s /bin/bash -u 1001 bb && \
    echo 'bb:bb' | chpasswd && \
    echo 'root:bb' | chpasswd

RUN mkdir -p /home/bb/.ssh /root/.ssh
COPY ./id_rsa.pub /home/bb/.ssh/authorized_keys
COPY ./id_rsa.pub /root/.ssh/authorized_keys

# Set up configuration for SSH
RUN mkdir /var/run/sshd
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# Expose the SSH port
EXPOSE 22

# Run SSH
CMD ["/usr/sbin/sshd", "-D"]
#CMD ["bash"]
