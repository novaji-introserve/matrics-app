# Console Security Protection Module

## Overview

This module protects your Odoo installation against console-based JavaScript execution attempts while preserving all legitimate Odoo functionality.

## Features

- ✅ Detects console-based code execution
- ✅ Blocks malicious console commands (alert, eval, cookie manipulation, etc.)
- ✅ Extensive Odoo whitelisting to prevent breaking functionality
- ✅ Configurable via system parameters
- ✅ Three modes: off, logging, protection
- ✅ Debug mode for developers
- ✅ Can be enabled/disabled without restart

## Installation

1. Copy this module to: `custom_addons/icomply_odoo/console_security/`
2. Update your addons list in Odoo
3. Install the module through Odoo Apps interface

## Configuration

### System Parameters

The module uses these system parameters (found in **Settings > Technical > Parameters > System Parameters**):

1. **console_security.enabled**
   - Values: `True` or `False`
   - Default: `True`
   - Enables/disables the entire protection system

2. **console_security.mode**
   - Values: `off`, `logging`, `protection`
   - Default: `protection`
   - **off**: Protection disabled
   - **logging**: Detect and log suspicious activity (no blocking)
   - **protection**: Full blocking of console commands

3. **console_security.debug_key**
   - Default: `CONSOLE_DEBUG_{system_random}`
   - Secret key for developers to bypass protection during debugging
   - **IMPORTANT**: Change this value in production!

### Usage Examples

#### Enable Protection Mode (Default)
```
console_security.enabled = True
console_security.mode = protection
```

#### Test Mode (Logging Only)
```
console_security.enabled = True
console_security.mode = logging
```

#### Disable Protection
```
console_security.enabled = False
# OR
console_security.mode = off
```

#### Developer Debug Mode

If you need to bypass protection for legitimate debugging:

1. Get your debug key from system parameter `console_security.debug_key`
2. Open browser console
3. Run: `window.enableDebug("YOUR_DEBUG_KEY")`
4. Protection will be bypassed for that session

## What Gets Blocked

When protection is enabled and in `protection` mode, these console commands are blocked:

- `alert()` - Popup alerts
- `eval()` - Code evaluation
- `Function()` constructor - Dynamic function creation
- `document.cookie = ...` - Cookie manipulation
- `location.href = 'javascript:...'` - JavaScript protocol navigation
- `document.write()` - Document writing
- `appendChild(<script>)` - Inline script injection
- `createElement('script')` - Script element creation
- `createElement('iframe')` with malicious content - Iframe injection

## What Is Whitelisted (Allowed)

All legitimate Odoo code is whitelisted and will work normally:

- Odoo 16 module system (`@odoo-module`, `@web/core`)
- Odoo framework components (`owl.Component`, `useState`, etc.)
- Odoo services (`registry.category`, `services.rpc`)
- Odoo models and ORM operations
- Odoo XML/QWeb templates
- Custom addons JavaScript (`icomply_odoo/*`)
- Legitimate event handlers (button clicks, form submissions)
- All Odoo built-in functionality

## How It Works

1. **Stack Trace Analysis**: Checks if code execution originates from Odoo files or browser console
2. **Pattern Matching**: Whitelists extensive Odoo patterns to ensure functionality
3. **Function Hooking**: Intercepts suspicious functions when called from console
4. **Mode-Based Response**: Logs or blocks based on configuration

## Testing

### Test 1: Verify Protection Works
1. Enable protection mode
2. Open browser console
3. Try: `alert("test")`
4. Should see: `🚫 [Console Security] BLOCKED: alert() from console`

### Test 2: Verify Odoo Still Works
1. Enable protection mode
2. Test normal Odoo functionality:
   - Create/edit records
   - Submit forms
   - Use Odoo buttons and actions
   - Custom addon features
3. Everything should work normally

### Test 3: Logging Mode
1. Set mode to `logging`
2. Try console commands
3. Check console for warnings (no blocking)
4. Verify Odoo functionality still works

## Troubleshooting

### Module Not Working

1. **Check if module is installed**
   - Go to Apps, search "Console Security"
   - Verify it's installed and activated

2. **Check system parameters**
   - Settings > Technical > Parameters > System Parameters
   - Verify `console_security.enabled` is `True`
   - Verify `console_security.mode` is not `off`

3. **Clear browser cache**
   - Hard refresh (Ctrl+Shift+R / Cmd+Shift+R)
   - Check browser console for initialization messages

### Odoo Functionality Broken

1. **Switch to logging mode**
   - Set `console_security.mode` to `logging`
   - This will show what's being detected without blocking
   - Report any false positives

2. **Disable temporarily**
   - Set `console_security.enabled` to `False`
   - Odoo should work normally

3. **Check console for errors**
   - Look for `[Console Security]` messages
   - Report any issues

### Need to Bypass During Development

Use debug mode:
```javascript
// In browser console
window.enableDebug("YOUR_DEBUG_KEY")
```

## Security Notes

⚠️ **Important**:
- This module prevents console-based attacks, not injected XSS
- Always use HTTPS and proper CSP headers (already configured in Apache)
- This is a defense-in-depth measure, not a replacement for proper security
- Keep your debug key secret and change it from default

## Support

For issues or questions:
- Check Odoo logs for errors
- Review browser console for warning messages
- Contact Novaji Introserve Ltd support

## License

LGPL-3




