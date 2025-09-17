# Known Issues

## Enhanced Generation Engine (Non-Critical)

### Issue: Async Generation Column Access Error
**Status**: Known issue with fallback working
**Impact**: Low - system automatically falls back to working original generation
**Error**: `'column_name'` errors in async generation engine
**Root Cause**: Column data access pattern in async context

### Issue: Event Loop Conflict  
**Status**: Known issue with fallback working
**Impact**: Low - system automatically falls back to working original generation
**Error**: "this event loop is already running"
**Root Cause**: FastAPI's event loop conflict with nested asyncio.run()

## Workarounds

1. **Use Basic Generation**: The original generation method works perfectly
2. **Automatic Fallback**: Enhanced generation automatically falls back to basic generation
3. **Full Functionality**: All core features work - only the enhanced concurrent generation has issues

## Current Working Features

✅ All web UI functionality  
✅ Basic AI description generation  
✅ Database schema migration  
✅ New enhanced data models  
✅ User context system  
✅ Table filtering and prioritization  
✅ Pagination and search APIs  
✅ ERD generation  

## Fix Priority

**Priority**: Low - The enhanced generation is an optimization feature, not core functionality. The system works perfectly with the original generation method.

## Temporary Solution

The enhanced generation will be fixed in a future update. For now, users can:
- Use the standard generation (works perfectly)
- Benefit from all other enhancements (pagination, filtering, user context, ERD)
- System automatically handles the fallback transparently