"""
Configures the Global Tracer Provider and exports the traces to the
OpenTelemetry Collector. The OpenTelemetry Collector is configured to
receive traces via OTLP over HTTP
The OTLP exporter is configured to use the environment variables
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT and OTEL_EXPORTER_OTLP_TRACES_HEADERS
to configure the endpoint and headers for the OTLP exporter.
The OTLP* environment variables are configured in the docker-compose.yaml
and podman-compose.yaml files for iib workers and api.

Usage:
    @instrument_tracing()
    def func():
        pass

    @instrument_tracing()
    class MyClass:
        def func1():
            pass
        def _func2():
            pass

"""
import functools
import inspect
import logging
import os
from flask import request
from typing import Dict
from opentelemetry import trace
from opentelemetry.trace import Tracer
from opentelemetry.trace import SpanKind, Status, StatusCode
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

log = logging.getLogger(__name__)
os.environ["OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"] = "http/protobuf"


class TracingWrapper:
    """
    Wrapper class that will wrap all methods of a calls with
    the instrument_tracing decorator.
    """

    def __init__(self, tracer: Tracer = None):
        if tracer is not None:
            self.tracer = tracer
        else:
            self.initialize_instrumentation()

    def initialize_instrumentation(self):
        """
        Initialize the instrumentation.
        """
        otlp_exporter = OTLPSpanExporter(
            endpoint="http://otel-collector-http-traces.apps.int.spoke.prod.us-east-1.aws.paas.redhat.com/v1/traces",  # noqa: E501
        )
        provider = TracerProvider(resource=Resource.create({SERVICE_NAME: "iib-workers"}))
        trace.set_tracer_provider(provider)
        self.tracer = trace.get_tracer(__name__)
        processor = BatchSpanProcessor(otlp_exporter)
        provider.add_span_processor(processor)

    def __getattr__(self, name):
        """
        Get the attribute from the tracer.

        :param name: The name of the attribute.
        :return: The attribute.
        """
        return getattr(self.tracer, name)


def instrument_tracing(
    func=None,
    *,
    service_name: str = "",
    span_name: str = "",
    ignoreTracing=False,
    attributes: Dict = None,
    existing_tracer: Tracer = None,
    is_class=False,
):
    """
    Decorator to instrument a function or class with tracing.
    :param func_or_class: The function or class to be decorated.
    :param service_name: The name of the service to be used.
    :param span_name: The name of the span to be created.
    :param ignoreTracing: If True, the function will not be traced.
    :param attributes: The attributes to be added to the span.
    :param existing_tracer: The tracer to be used.
    :return: The decorated function or class.
    """

    def instrument_class(cls):
        """
        Filters out all the methods that are to be instrumented
        for a class with tracing.

        :param cls: The class to be decorated.
        :return: The decorated class.
        """
        for name, method in cls.__dict__.items():
            if (
                callable(method)
                and not method.__name__.startswith("_")
                and not inspect.isclass(method)
            ):
                setattr(cls, name, instrument_tracing(method))
        return cls

    def instrument_span(func):
        log.info(f"Instrumenting span for {span_name}")
        propagator = TraceContextTextMapPropagator()
        tracer = trace.get_tracer(__name__)
        if tracer is None:
            print("Tracer is none")
            wrapper = TracingWrapper()
            tracer = wrapper.tracer
        context = None
        # if trace.get_current_span() is not None:
        #     context = trace.get_current_span().get_span_context()
        #     attributes = trace.get_current_span().attributes
        #     print(f"context is {context}")
        #     print(f"attributes is {attributes}")

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            
            # from iib.web.api_v1 import headers
            # if headers:
            #     traceparent = headers.get('traceparent')
            # # headers = get_headers()
            # # log.info(f"Headers: {headers}")
            #     carier = {"traceparent": traceparent}
            #     context = propagator.extract(carier)
            with tracer.start_as_current_span(
                span_name or func.__name__,
                kind=SpanKind.SERVER,
                context=context or kwargs.get("context", None),
            ) as span:
                span.set_attribute("function_name", func.__name__)
                if func.__name__:  # If the function has a name
                    span.set_attribute("function_name", func.__name__)
                    print(func.__name__)
                try:
                    result = func(*args, **kwargs)
                except Exception as exc:
                    span.set_status(Status(StatusCode.ERROR))
                    span.record_exception(exc)
                    raise
                else:
                    if result is not None:
                        span.set_attribute("result_attributes", result)
                    if args:
                        span.set_attribute("arguments", args)
                    if kwargs:
                        # Need to handle all the types of kwargs
                        for keys, values in kwargs.items():
                            if keys == 'context':
                                continue
                            if type(values) is dict:
                                for key, value in values.items():
                                    span.set_attribute(k, v)
                            elif type(values) is list:
                                for value in values:
                                    if type(value) is dict:
                                        for k, v in value.items():
                                            span.set_attribute(k, v)
                                    else:
                                        span.set_attribute(keys, value)
                            else:
                                span.set_attribute(keys, values)
                    if func.__doc__:
                        span.set_attribute("description", func.__doc__)
                    span.add_event(f"{func.__name__} executed", {"result": result or "success"})
                    span.set_status(Status(StatusCode.OK))
                finally:
                    # Add the span context from the current span to the link
                    span_id = span.get_span_context().span_id
                    trace_id = span.get_span_context().trace_id
                    # Syntax of traceparent is f"00-{trace_id}-{span_id}-01"
                    traceparent = f"00-{trace_id}-{span_id}-01"
                    headers = {'traceparent': traceparent}
                    propagator.inject(span.get_span_context(), headers)
                    log.info("Headers are: %s", headers)

                return result

        wrapper = wrapper
        return wrapper

    if ignoreTracing:
        return func

    if is_class:
        # The decorator is being used to decorate a function
        return instrument_class
    else:
        # The decorator is being used to decorate a function
        return instrument_span


 # headers = get_headers()
            # traceparent = headers.get("traceparent")
            # traceparent = request.headers.get("traceparent")
            # carier = {"traceparent": traceparent}
            # context = propagator.extract(carier)
            # log.info(f"Context are: {context}")
            # headers = get_headers()