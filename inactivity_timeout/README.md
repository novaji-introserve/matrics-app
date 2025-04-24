# Inactivity Timeout for Odoo

## Overview

This module automatically logs out users after a configurable period of inactivity, enhancing security and compliance for your Odoo installation. The module is fully configurable through Odoo's system parameters, making it easy to adjust for different security needs.

## Features

- **Configurable Timeout Intervals**: Set both the total inactivity time and warning time through system parameters
- **Warning Notification**: Users receive a notification before being logged out, with the option to stay logged in
- **Activity Detection**: Resets the timer based on user activity (mouse movements, keyboard input, etc.)
- **Easy Administration**: Configure through the standard Odoo interface
- **Clean Architecture**: Built following Single Responsibility Principle (SRP) and Don't Repeat Yourself (DRY) principles

## Configuration

After installing the module, you can configure it through the standard **Settings > Technical > Parameters > System Parameters** menu.

The following system parameters are available:

- `inactivity_timeout.timeout`: Total inactivity time before logout (in seconds, default: 300)
- `inactivity_timeout.warning`: Warning time before logout (in seconds, default: 60)

## Technical Implementation

The module implements:

- A JavaScript service that monitors user activity
- A controller that provides parameters and handles session expiration
- Proper separation of concerns between UI, logic, and configuration
- Fallback mechanisms for notification display

## Requirements

- Odoo 16.0 or later

## Installation

1. Copy this module to your Odoo addons directory
2. Update your addons list
3. Install the module through the Odoo interface

## License

LGPL-3.0

## Support

For support, contact Synth corp.
