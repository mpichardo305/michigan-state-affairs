#!/usr/bin/env bash
#!/bin/bash

# Prevent macOS from sleeping for 2 minutes
# This ensures the machine is awake when cron triggers the actual job

caffeinate -t 120 &