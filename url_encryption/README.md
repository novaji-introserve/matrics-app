# URL Encryption for Odoo

## Overview

This module provides advanced URL encryption functionality for Odoo, automatically encrypting sensitive URL parameters to enhance security and prevent unauthorized access or manipulation. The module transparently encrypts parameters like action IDs, menu IDs, company IDs, and other sensitive data while maintaining full Odoo functionality.

## Features

- **Automatic URL Encryption**: Encrypts sensitive parameters in real-time as users navigate
- **Transparent Operation**: Works seamlessly without affecting user experience
- **Comprehensive Parameter Coverage**: Encrypts action IDs, menu IDs, company IDs, model names, and more
- **Strong Encryption**: Uses Fernet encryption (AES 128) for robust security
- **Multi-Company Support**: Properly handles company switching with encrypted company IDs
- **Real-time Processing**: Encrypts URLs dynamically during navigation
- **Cache Optimization**: Implements intelligent caching to improve performance
- **Odoo 16 Optimized**: Specifically designed for Odoo 16 architecture

## Security Benefits

- **Prevents Parameter Manipulation**: Users cannot modify action IDs, menu IDs, or other sensitive parameters
- **Hides System Architecture**: Conceals internal system structure and ID patterns
- **Access Control Enhancement**: Reduces risk of unauthorized access through URL manipulation
- **Audit Trail Protection**: Prevents inference of system usage patterns from URLs
- **Multi-Company Security**: Protects company-specific data from cross-company access attempts

## Technical Implementation

### Encrypted Parameters

The module automatically encrypts the following URL parameters:

- `action` - Action IDs
- `menu_id` - Menu identifiers
- `cids` - Company IDs (multi-company support)
- `id` - Record identifiers
- `active_id` - Active record ID
- `active_ids` - Multiple active record IDs
- `model` - Model names
- `view_type` - View type specifications

### Architecture

- **Service-based Design**: Implements as an Odoo service for optimal integration
- **Event-driven Encryption**: Monitors URL changes and encrypts automatically
- **Robust Decryption**: Handles encrypted tokens seamlessly during page loads
- **Action Manager Integration**: Patches action manager for proper parameter handling
- **Router Compatibility**: Full integration with Odoo's routing system

### Example Transformation

**Before Encryption:**

```sh
https://yourdomain.com/web#action=144&cids=1&menu_id=98&model=sale.order
```

**After Encryption:**

```sh
https://yourdomain.com/web#token=gAAAAABhK2x...encrypted_data_here...xyz
```

## Installation

### Prerequisites

- Odoo 16.0 or later
- Python `cryptography` library

### Installation Steps

1. **Install Dependencies**:

   ```bash
   pip install cryptography
   ```

2. **Download and Extract**: Place the module in your Odoo addons directory:

   ```sh
   /path/to/odoo/addons/url_encryption/
   ```

3. **Update Module List**:
   - Go to Apps → Update Apps List
   - Or restart Odoo with `--update=all`

4. **Install Module**:
   - Navigate to Apps
   - Search for "URL Encryption"
   - Click Install

### Module Structure

```sh
url_encryption/
├── __init__.py
├── __manifest__.py
├── controllers/
│   ├── __init__.py
│   └── main.py
├── models/
│   ├── __init__.py
│   └── ir_config_parameter.py
├── static/src/js/
│   └── url_encryption.js
└── security/
    └── ir.model.access.csv
```

## Configuration

### Automatic Setup

The module automatically:

- Generates a unique encryption key on first installation
- Stores the key securely in Odoo's configuration parameters
- Initializes all necessary services and components

### Manual Configuration (Optional)

Access **Settings → Technical → Parameters → System Parameters** to view:

- `url_encryption.key` - The encryption key (view only, auto-generated)

### Customization

To encrypt additional parameters, modify the `encrypt_keys` list in `/controllers/main.py`:

```python
encrypt_keys = ['action', 'menu_id', 'id', 'active_id', 'active_ids', 'model', 'cids', 'your_custom_param']
```

## Performance Considerations

- **Optimized Caching**: Implements intelligent caching to minimize encryption overhead
- **Asynchronous Processing**: Uses async operations to prevent UI blocking
- **Minimal Latency**: Typically adds <50ms to navigation time
- **Memory Efficient**: Lightweight implementation with minimal memory footprint

## Compatibility

### Odoo Versions

- ✅ Odoo 16.0 (Fully Supported)
- ✅ Odoo 15.0 (Compatible with minor modifications)
- ❓ Odoo 17.0+ (Contact for compatibility updates)

### Browser Support

- ✅ Chrome 80+
- ✅ Firefox 75+
- ✅ Safari 13+
- ✅ Edge 80+

### Third-party Module Compatibility

- ✅ Multi-Company modules
- ✅ Custom action modules
- ✅ Dashboard modules
- ✅ Report modules

## Troubleshooting

### Common Issues

**URLs not encrypting:**

1. Clear browser cache and reload
2. Check browser console for JavaScript errors
3. Verify module is properly installed and activated
4. Restart Odoo server

**Decryption errors:**

1. Check server logs for encryption/decryption errors
2. Verify `cryptography` library is installed
3. Ensure encryption key is properly generated

**Performance issues:**

1. Monitor server resources during heavy usage
2. Check cache hit rates in browser dev tools
3. Consider increasing server memory if needed

### Debug Mode

Enable debug mode to see detailed logging:

```python
import logging
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)
```

### Support Commands

Check module status:

```python
# In Odoo shell
env['ir.module.module'].search([('name', '=', 'url_encryption')])
```

Regenerate encryption key:

```python
# In Odoo shell (use with caution)
env['ir.config_parameter'].set_param('url_encryption.key', '')
```

## Security Notes

- **Key Management**: Encryption keys are stored securely in Odoo's database
- **Session Security**: Encrypted tokens are session-specific and time-limited
- **Data Protection**: No sensitive data is logged or cached in plaintext
- **Access Control**: Only authenticated users can encrypt/decrypt URLs

## Changelog

### Version 1.0.0

- Initial release for Odoo 16
- Complete URL parameter encryption
- Multi-company support
- Performance optimizations
- Comprehensive error handling

## License

**LGPL-3.0** - This module is licensed under the GNU Lesser General Public License v3.0

## Support & Maintenance

### Commercial Support

- Priority bug fixes
- Custom feature development
- Integration assistance
- Performance optimization
- 24/7 technical support

### Community Support

- GitHub issues for bug reports
- Community forums for general questions
- Documentation updates
- Feature suggestions

### Contact Information

- **Email**: <insights-synth@tech-center.com>
- **Website**: <https://www.cybercraftsmen.tech>
- **Documentation**: <https://github.com/Synth-corp/url_encryption>
- **GitHub**: <https://github.com/Synth-corp/url_encryption>

## Contributing

We welcome contributions! Please:

1. Fork the repository
2. Create a feature branch
3. Submit a pull request with detailed description
4. Include tests for new functionality

## Warranty & Disclaimer

This module is provided "as is" without warranty of any kind. Users are responsible for testing in their specific environment before production deployment.

---

**© 2025 Synth Corp. All rights reserved.**

*Transform your Odoo security with professional URL encryption.*
