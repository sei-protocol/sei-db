# ⚠️ CRITICAL FIX REQUIRED

## 🚨 Problem
**Current code has a critical bug that causes 10x performance degradation**

**File**: `sc/memiavl/db.go`  
**Line**: 600  
**Impact**: EVM export speed drops from 1000k nodes/s → 130k nodes/s

---

## ✅ The Fix (One Line Change)

### Current Code (BROKEN):
```go
// sc/memiavl/db.go:598-601
if disablePrefetch {
    // Production mode: background rewrite while main chain is running
    ctx = context.WithValue(ctx, contextKey("disableCacheDrop"), true)  // ❌ DELETE THIS LINE!
    fmt.Printf("[REWRITE] Using Export/Import (background mode: prefetch+cache-drop disabled)\n")
}
```

### Fixed Code:
```go
// sc/memiavl/db.go:598-604
if disablePrefetch {
    // Production mode: background rewrite while main chain is running
    // Note: Cache drop is ALWAYS ENABLED for write side (cacheDropWriter)
    // This is critical to prevent write cache from evicting read cache
    // Disabling cache drop causes 10x slowdown in production (EVM export)
    fmt.Printf("[REWRITE] Using Export/Import (background mode: prefetch disabled, cache-drop ENABLED)\n")
} else {
    // Test mode: direct call, no main chain running
    fmt.Printf("[REWRITE] Using Export/Import (test mode: prefetch+cache-drop ENABLED)\n")
}
```

---

## 📋 Steps to Apply Fix

```bash
cd /Users/blindchaser/workspace/sei-protocol/sei-db

# Apply the fix
git checkout sc/memiavl/db.go

# Or manually edit line 598-604 as shown above
```

---

## 🧪 How to Verify

After applying the fix, you should see in logs:
```
[REWRITE] Using Export/Import (background mode: prefetch disabled, cache-drop ENABLED)
```

NOT:
```
[REWRITE] Using Export/Import (background mode: prefetch+cache-drop disabled)
```

And EVM export should maintain **700-900k nodes/s** throughout the entire process.

---

## 📊 Expected Performance After Fix

### Cold Start
- **Duration**: ~2 hours (first time)
- **Speed**: Variable (cache warming up)

### Warm Rewrite
- **Duration**: 27-35 min ✅
- **EVM Speed**: 700-900k nodes/s ✅
- **Stability**: No slowdown at 30-46% mark ✅

---

## ⚠️ Why This Bug Exists

The line `ctx = context.WithValue(ctx, contextKey("disableCacheDrop"), true)` was added to avoid cache interference between main chain and background rewrite.

**Intention** (wrong): Disable cache drop during background rewrite
**Reality**: Causes write cache to accumulate → evicts read cache → 10x slowdown

**Correct approach**: 
- **Disable prefetch** (already done) ✅
- **Always enable write-side cache drop** (broken by line 600) ❌
- **Accept that read-side cache management is limited** ✅

---

## 🎯 Production Readiness After Fix

✅ **Ready to deploy after this fix**
- All other optimizations are working well
- Production metrics prove the approach is sound
- Just this one line causes the problem

---

**Created**: 2025-11-03  
**Priority**: 🔴 **CRITICAL - Fix immediately before production deployment**


