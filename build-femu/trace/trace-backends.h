/* This file is autogenerated by tracetool, do not edit. */

#ifndef TRACE_BACKENDS_GENERATED_TRACERS_H
#define TRACE_BACKENDS_GENERATED_TRACERS_H

#include "trace/control.h"

extern TraceEvent _TRACE_DBUS_VMSTATE_PRE_SAVE_EVENT;
extern TraceEvent _TRACE_DBUS_VMSTATE_POST_LOAD_EVENT;
extern TraceEvent _TRACE_DBUS_VMSTATE_LOADING_EVENT;
extern TraceEvent _TRACE_DBUS_VMSTATE_SAVING_EVENT;
extern uint16_t _TRACE_DBUS_VMSTATE_PRE_SAVE_DSTATE;
extern uint16_t _TRACE_DBUS_VMSTATE_POST_LOAD_DSTATE;
extern uint16_t _TRACE_DBUS_VMSTATE_LOADING_DSTATE;
extern uint16_t _TRACE_DBUS_VMSTATE_SAVING_DSTATE;
#define TRACE_DBUS_VMSTATE_PRE_SAVE_ENABLED 1
#define TRACE_DBUS_VMSTATE_POST_LOAD_ENABLED 1
#define TRACE_DBUS_VMSTATE_LOADING_ENABLED 1
#define TRACE_DBUS_VMSTATE_SAVING_ENABLED 1
#include "qemu/log-for-trace.h"


#define TRACE_DBUS_VMSTATE_PRE_SAVE_BACKEND_DSTATE() ( \
    trace_event_get_state_dynamic_by_id(TRACE_DBUS_VMSTATE_PRE_SAVE) || \
    false)

static inline void _nocheck__trace_dbus_vmstate_pre_save(void)
{
    if (trace_event_get_state(TRACE_DBUS_VMSTATE_PRE_SAVE) && qemu_loglevel_mask(LOG_TRACE)) {
        struct timeval _now;
        gettimeofday(&_now, NULL);
        qemu_log("%d@%zu.%06zu:dbus_vmstate_pre_save "  "\n",
                 qemu_get_thread_id(),
                 (size_t)_now.tv_sec, (size_t)_now.tv_usec
                 );
    }
}

static inline void trace_dbus_vmstate_pre_save(void)
{
    if (true) {
        _nocheck__trace_dbus_vmstate_pre_save();
    }
}

#define TRACE_DBUS_VMSTATE_POST_LOAD_BACKEND_DSTATE() ( \
    trace_event_get_state_dynamic_by_id(TRACE_DBUS_VMSTATE_POST_LOAD) || \
    false)

static inline void _nocheck__trace_dbus_vmstate_post_load(int version_id)
{
    if (trace_event_get_state(TRACE_DBUS_VMSTATE_POST_LOAD) && qemu_loglevel_mask(LOG_TRACE)) {
        struct timeval _now;
        gettimeofday(&_now, NULL);
        qemu_log("%d@%zu.%06zu:dbus_vmstate_post_load " "version_id: %d" "\n",
                 qemu_get_thread_id(),
                 (size_t)_now.tv_sec, (size_t)_now.tv_usec
                 , version_id);
    }
}

static inline void trace_dbus_vmstate_post_load(int version_id)
{
    if (true) {
        _nocheck__trace_dbus_vmstate_post_load(version_id);
    }
}

#define TRACE_DBUS_VMSTATE_LOADING_BACKEND_DSTATE() ( \
    trace_event_get_state_dynamic_by_id(TRACE_DBUS_VMSTATE_LOADING) || \
    false)

static inline void _nocheck__trace_dbus_vmstate_loading(const char * id)
{
    if (trace_event_get_state(TRACE_DBUS_VMSTATE_LOADING) && qemu_loglevel_mask(LOG_TRACE)) {
        struct timeval _now;
        gettimeofday(&_now, NULL);
        qemu_log("%d@%zu.%06zu:dbus_vmstate_loading " "id: %s" "\n",
                 qemu_get_thread_id(),
                 (size_t)_now.tv_sec, (size_t)_now.tv_usec
                 , id);
    }
}

static inline void trace_dbus_vmstate_loading(const char * id)
{
    if (true) {
        _nocheck__trace_dbus_vmstate_loading(id);
    }
}

#define TRACE_DBUS_VMSTATE_SAVING_BACKEND_DSTATE() ( \
    trace_event_get_state_dynamic_by_id(TRACE_DBUS_VMSTATE_SAVING) || \
    false)

static inline void _nocheck__trace_dbus_vmstate_saving(const char * id)
{
    if (trace_event_get_state(TRACE_DBUS_VMSTATE_SAVING) && qemu_loglevel_mask(LOG_TRACE)) {
        struct timeval _now;
        gettimeofday(&_now, NULL);
        qemu_log("%d@%zu.%06zu:dbus_vmstate_saving " "id: %s" "\n",
                 qemu_get_thread_id(),
                 (size_t)_now.tv_sec, (size_t)_now.tv_usec
                 , id);
    }
}

static inline void trace_dbus_vmstate_saving(const char * id)
{
    if (true) {
        _nocheck__trace_dbus_vmstate_saving(id);
    }
}
#endif /* TRACE_BACKENDS_GENERATED_TRACERS_H */
