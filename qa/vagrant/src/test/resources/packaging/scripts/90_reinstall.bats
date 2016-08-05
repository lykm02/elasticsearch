#!/usr/bin/env bats

# Tests upgrading elasticsearch from a previous version with the deb or rpm
# packages. Just uses a single node cluster on the current machine rather than
# fancy rolling restarts.

# WARNING: This testing file must be executed as root and can
# dramatically change your system. It removes the 'elasticsearch'
# user/group and also many directories. Do not execute this file
# unless you know exactly what you are doing.

# The test case can be executed with the Bash Automated
# Testing System tool available at https://github.com/sstephenson/bats
# Thanks to Sam Stephenson!

# Licensed to Elasticsearch under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Load test utilities
load packaging_test_utils
load os_package

# Cleans everything for the 1st execution
setup() {
    skip_not_dpkg_or_rpm
}

@test "[REINSTALL] install" {
    clean_before_test
    install_package
}

@test "[REINSTALL] purge elasticsearch" {
    purge_elasticsearch
}

@test "[REINSTALL] chown directories" {
    # to simulate the loss of ownership
    if [ -d /var/lib/elasticsearch ]; then
      sudo chown -R root:root /var/lib/elasticsearch
    fi
    if [ -d "/var/log/elasticsearch" ]; then
      sudo chown -R root:root /var/log/elasticsearch
    fi
    if [ -d /etc/elasticsearch ]; then
      sudo chown -R root:root /etc/elasticsearch
    fi
}

@test "[REINSTALL] reinstall elasticsearch" {
    install_package
}

@test "[REINSTALL] check ownership" {
    assert_recursive_ownership /var/lib/elasticsearch elasticsearch elasticsearch
    assert_recursive_ownership /var/log/elasticsearch elasticsearch elasticsearch
    assert_recursive_ownership /etc/elasticsearch root elasticsearch
}
