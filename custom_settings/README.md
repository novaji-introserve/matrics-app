# Custom Settings Layout

## Overview

This Odoo module simplifies the Settings interface by showing only the General Settings tab in the sidebar and hiding specific sections within the General Settings page to create a cleaner, more focused administrative experience.

![Custom Settings Layout](static/description/banner.png)

## Features

- **Simplified Navigation**: Only displays the "General Settings" tab in the settings sidebar
- **Reduced Complexity**: Hides unnecessary sections from the General Settings page
- **Customizable**: Can be modified to show/hide specific sections based on your needs
- **Performance Optimized**: Uses both JavaScript and CSS to ensure reliable functionality

## Hidden Sections

The following sections are hidden from the General Settings page:

- Discuss
- Statistics
- Contacts
- Permissions
- Integrations
- Developer Tools
- About
- Performance

## Visible Sections

The following sections remain visible:

- Users
- Languages
- Companies
- Backend Theme

## Installation

### From the Odoo App Store

1. Navigate to Apps > Search for "Custom Settings Layout"
2. Click Install

### Manual Installation

1. Download this module and place it in your Odoo addons directory
2. Update your addons list: Settings > Updates > Update Apps List
3. Search for "Custom Settings Layout" and install

## Technical Details

### Implementation Method

The module uses two approaches to ensure sections are properly hidden:

1. **JavaScript**: Dynamically identifies and hides elements based on content and structure
2. **CSS**: Uses !important rules to override default visibility settings

### File Structure

```bash
custom_settings/
├── __init__.py
├── __manifest__.py
├── static/
│   ├── description/
│   │   └── banner.png
│   ├── src/
│       ├── js/
│       │   └── settings_form.js
│       └── scss/
│           └── custom_settings.scss
```

### Dependencies

- base
- base_setup
- web

## Configuration

No additional configuration is needed after installation. The module automatically applies the customizations to the Settings page.

## Version Compatibility

- Odoo 15.0: ✓
- Odoo 16.0: ✓
- Odoo 17.0: ✓

## Troubleshooting

### Known Issues

- If you have other modules that modify the settings page, they may conflict with this module
- Custom themes may affect the visibility changes

### Solutions

- Install this module after any other modules that modify the settings page
- If some sections are still visible, try clearing your browser cache

## Support

For technical support or customization requests:

- Email: <olumide.awodeji@hotmail.com>  
- Website: <https://cybercraftsmen.tech>

## License

This module is licensed under LGPL-3.

## Author

Olumide Awodeji (Synth corp)  
Website: <https://cybercraftsmen.tech>

---

*Note: This module is not affiliated with or endorsed by Odoo SA. The module is provided as-is without any warranty.*
