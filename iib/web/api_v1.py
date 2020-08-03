# SPDX-License-Identifier: GPL-3.0-or-later
import copy
import os
from datetime import datetime

import flask
import kombu
from flask_login import current_user, login_required
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.sql import text
from werkzeug.exceptions import Forbidden, Gone, NotFound

from iib.exceptions import IIBError, ValidationError
from iib.web import db, messaging
from iib.web.errors import handle_broker_error, handle_broker_batch_error
from iib.web.models import (
    Architecture,
    Batch,
    Image,
    Operator,
    Request,
    RequestAdd,
    RequestRegenerateBundle,
    RequestRm,
    RequestState,
    RequestStateMapping,
    get_request_query_options,
)
from iib.web.utils import pagination_metadata, str_to_bool
from iib.workers.tasks.build import (
    handle_add_request,
    handle_regenerate_bundle_request,
    handle_rm_request,
)
from iib.workers.tasks.general import failed_request_callback

api_v1 = flask.Blueprint('api_v1', __name__)


@api_v1.route('/builds/<int:request_id>')
def get_build(request_id):
    """
    Retrieve the build request.

    :param int request_id: the request ID that was passed in through the URL.
    :rtype: flask.Response
    :raise NotFound: if the request is not found
    """
    # Create an alias class to load the polymorphic classes
    poly_request = with_polymorphic(Request, '*')
    query = poly_request.query.options(*get_request_query_options(verbose=True))
    return flask.jsonify(query.get_or_404(request_id).to_json())


@api_v1.route('/builds/<int:request_id>/logs')
def get_build_logs(request_id):
    """
    Retrieve the logs for the build request.

    :param int request_id: the request ID that was passed in through the URL.
    :rtype: flask.Response
    :raise NotFound: if the request is not found or there are no logs for the request
    :raise Gone: if the logs for the build request have been removed due to expiration
    """
    request_log_dir = flask.current_app.config['IIB_REQUEST_LOGS_DIR']
    if not request_log_dir:
        raise NotFound()

    request = Request.query.get_or_404(request_id)
    log_file_path = os.path.join(request_log_dir, f'{request_id}.log')
    if not os.path.exists(log_file_path):
        expired = request.logs_expiration < datetime.utcnow()
        if expired:
            raise Gone(f'The logs for the build request {request_id} no longer exist')
        finalized = request.state.state_name in RequestStateMapping.get_final_states()
        if finalized:
            raise NotFound()
        # The request may not have been initiated yet. Return empty logs until it's processed.
        return flask.Response('', mimetype='text/plain')

    with open(log_file_path) as f:
        return flask.Response(f.read(), mimetype='text/plain')


@api_v1.route('/builds')
def get_builds():
    """
    Retrieve the paginated build requests.

    :rtype: flask.Response
    """
    batch_id = flask.request.args.get('batch')
    state = flask.request.args.get('state')
    verbose = str_to_bool(flask.request.args.get('verbose'))
    max_per_page = flask.current_app.config['IIB_MAX_PER_PAGE']

    # Create an alias class to load the polymorphic classes
    poly_request = with_polymorphic(Request, '*')
    query = poly_request.query.options(*get_request_query_options(verbose=verbose))
    if state:
        RequestStateMapping.validate_state(state)
        state_int = RequestStateMapping.__members__[state].value
        query = query.join(Request.state)
        query = query.filter(RequestState.state == state_int)

    if batch_id is not None:
        batch_id = Batch.validate_batch(batch_id)
        query = query.filter_by(batch_id=batch_id)

    pagination_query = query.order_by(Request.id.desc()).paginate(max_per_page=max_per_page)
    requests = pagination_query.items

    query_params = {}
    if state:
        query_params['state'] = state
    if verbose:
        query_params['verbose'] = verbose
    if batch_id:
        query_params['batch'] = batch_id

    response = {
        'items': [request.to_json(verbose=verbose) for request in requests],
        'meta': pagination_metadata(pagination_query, **query_params),
    }
    return flask.jsonify(response)


@api_v1.route('/healthcheck')
def get_healthcheck():
    """
    Respond to a health check.

    :rtype: flask.Response
    :return: json object representing the health of IIB
    :raises IIBError: if the database connection fails
    """
    # Test DB connection
    try:
        db.engine.execute(text('SELECT 1'))
    except Exception:
        flask.current_app.logger.exception('DB test failed.')
        raise IIBError('Database health check failed.')

    return flask.jsonify({'status': 'Health check OK'})


def _should_force_overwrite():
    """
    Determine if the ``overwrite_from_index`` parameter should be forced.

    This is for clients that require this functionality but do not currently use the
    ``overwrite_from_index`` parameter already.

    :return: the boolean that determines if the overwrite should be forced
    :rtype: bool
    """
    # current_user.is_authenticated is only ever False when auth is disabled
    if not current_user.is_authenticated:
        return False
    privileged_users = flask.current_app.config['IIB_PRIVILEGED_USERNAMES']
    force_ovewrite = flask.current_app.config['IIB_FORCE_OVERWRITE_FROM_INDEX']

    should_force = current_user.username in privileged_users and force_ovewrite
    if should_force:
        flask.current_app.logger.info(
            'The "overwrite_from_index" parameter is being forced to True'
        )

    return should_force


def _get_user_queue(serial=False):
    """
    Return the name of the celery task queue mapped to the current user.

    :param bool serial: whether or not the task must run serially
    :return: queue name to be used or None if the default queue should be used
    :rtype: str or None
    """
    # current_user.is_authenticated is only ever False when auth is disabled
    if not current_user.is_authenticated:
        return

    username = current_user.username
    if serial:
        labeled_username = f'SERIAL:{username}'
    else:
        labeled_username = f'PARALLEL:{username}'

    queue = flask.current_app.config['IIB_USER_TO_QUEUE'].get(labeled_username)
    if not queue:
        queue = flask.current_app.config['IIB_USER_TO_QUEUE'].get(username)
    return queue


@api_v1.route('/builds/add', methods=['POST'])
@login_required
def add_bundles():
    """
    Submit a request to add operator bundles to an index image.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload = flask.request.get_json()
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    request = RequestAdd.from_json(payload)
    db.session.add(request)
    db.session.commit()
    messaging.send_message_for_state_change(request, new_batch_msg=True)

    overwrite_from_index = _should_force_overwrite() or payload.get('overwrite_from_index')
    celery_queue = _get_user_queue(serial=overwrite_from_index)
    args = [
        payload['bundles'],
        payload['binary_image'],
        request.id,
        payload.get('from_index'),
        payload.get('add_arches'),
        payload.get('cnr_token'),
        payload.get('organization'),
        payload.get('force_backport'),
        overwrite_from_index,
        payload.get('overwrite_from_index_token'),
        flask.current_app.config['IIB_GREENWAVE_CONFIG'].get(celery_queue),
    ]
    safe_args = copy.copy(args)
    if payload.get('cnr_token'):
        safe_args[safe_args.index(payload['cnr_token'])] = '*****'
    if payload.get('overwrite_from_index_token'):
        safe_args[safe_args.index(payload['overwrite_from_index_token'])] = '*****'

    error_callback = failed_request_callback.s(request.id)

    try:
        handle_add_request.apply_async(
            args=args, link_error=error_callback, argsrepr=repr(safe_args), queue=celery_queue
        )
    except kombu.exceptions.OperationalError:
        handle_broker_error(request)

    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201


@api_v1.route('/builds/<int:request_id>', methods=['PATCH'])
@login_required
def patch_request(request_id):
    """
    Modify the given request.

    :param int request_id: the request ID from the URL
    :return: a Flask JSON response
    :rtype: flask.Response
    :raise Forbidden: If the user trying to patch a request is not an IIB worker
    :raise NotFound: if the request is not found
    :raise ValidationError: if the JSON is invalid
    """
    allowed_users = flask.current_app.config['IIB_WORKER_USERNAMES']
    # current_user.is_authenticated is only ever False when auth is disabled
    if current_user.is_authenticated and current_user.username not in allowed_users:
        raise Forbidden('This API endpoint is restricted to IIB workers')

    payload = flask.request.get_json()
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    if not payload:
        raise ValidationError('At least one key must be specified to update the request')

    request = Request.query.get_or_404(request_id)

    invalid_keys = payload.keys() - request.get_mutable_keys()
    if invalid_keys:
        raise ValidationError(
            'The following keys are not allowed: {}'.format(', '.join(invalid_keys))
        )

    for key, value in payload.items():
        if key == 'arches':
            Architecture.validate_architecture_json(value)
        elif key == 'bundle_mapping':
            exc_msg = f'The "{key}" key must be an object with the values as lists of strings'
            if not isinstance(value, dict):
                raise ValidationError(exc_msg)
            for v in value.values():
                if not isinstance(v, list) or any(not isinstance(s, str) for s in v):
                    raise ValidationError(exc_msg)
        elif not value or not isinstance(value, str):
            raise ValidationError(f'The value for "{key}" must be a non-empty string')

    if 'state' in payload and 'state_reason' not in payload:
        raise ValidationError('The "state_reason" key is required when "state" is supplied')
    elif 'state_reason' in payload and 'state' not in payload:
        raise ValidationError('The "state" key is required when "state_reason" is supplied')

    state_updated = False
    if 'state' in payload and 'state_reason' in payload:
        RequestStateMapping.validate_state(payload['state'])
        new_state = payload['state']
        new_state_reason = payload['state_reason']
        # This is to protect against a Celery task getting executed twice and setting the
        # state each time
        if request.state.state == new_state and request.state.state_reason == new_state_reason:
            flask.current_app.logger.info('Not adding a new state since it matches the last state')
        else:
            request.add_state(new_state, new_state_reason)
            state_updated = True

    image_keys = (
        'binary_image_resolved',
        'bundle_image',
        'from_bundle_image_resolved',
        'from_index_resolved',
        'index_image',
    )
    for key in image_keys:
        if key not in payload:
            continue
        key_value = payload.get(key, None)
        key_object = Image.get_or_create(key_value)
        # SQLAlchemy will not add the object to the database if it's already present
        setattr(request, key, key_object)

    for arch in payload.get('arches', []):
        request.add_architecture(arch)

    for operator, bundles in payload.get('bundle_mapping', {}).items():
        operator_img = Operator.get_or_create(operator)
        for bundle in bundles:
            bundle_img = Image.get_or_create(bundle)
            bundle_img.operator = operator_img

    db.session.commit()

    if state_updated:
        messaging.send_message_for_state_change(request)

    if current_user.is_authenticated:
        flask.current_app.logger.info(
            'The user %s patched request %d', current_user.username, request.id
        )
    else:
        flask.current_app.logger.info('An anonymous user patched request %d', request.id)

    return flask.jsonify(request.to_json()), 200


@api_v1.route('/builds/rm', methods=['POST'])
@login_required
def rm_operators():
    """
    Submit a request to remove operators from an index image.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload = flask.request.get_json()
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    request = RequestRm.from_json(payload)
    db.session.add(request)
    db.session.commit()
    messaging.send_message_for_state_change(request, new_batch_msg=True)

    overwrite_from_index = _should_force_overwrite() or payload.get('overwrite_from_index')
    args = [
        payload['operators'],
        payload['binary_image'],
        request.id,
        payload['from_index'],
        payload.get('add_arches'),
        overwrite_from_index,
        payload.get('overwrite_from_index_token'),
    ]

    safe_args = copy.copy(args)
    if payload.get('overwrite_from_index_token'):
        safe_args[safe_args.index(payload['overwrite_from_index_token'])] = '*****'

    error_callback = failed_request_callback.s(request.id)
    try:
        handle_rm_request.apply_async(
            args=args,
            link_error=error_callback,
            argsrepr=repr(safe_args),
            queue=_get_user_queue(serial=overwrite_from_index),
        )
    except kombu.exceptions.OperationalError:
        handle_broker_error(request)

    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201


@api_v1.route('/builds/regenerate-bundle', methods=['POST'])
@login_required
def regenerate_bundle():
    """
    Submit a request to regenerate an operator bundle image.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload = flask.request.get_json()
    if not isinstance(payload, dict):
        raise ValidationError('The input data must be a JSON object')

    request = RequestRegenerateBundle.from_json(payload)
    db.session.add(request)
    db.session.commit()
    messaging.send_message_for_state_change(request, new_batch_msg=True)

    error_callback = failed_request_callback.s(request.id)
    try:
        handle_regenerate_bundle_request.apply_async(
            args=[payload['from_bundle_image'], payload.get('organization'), request.id],
            link_error=error_callback,
            queue=_get_user_queue(),
        )
    except kombu.exceptions.OperationalError:
        handle_broker_error(request)

    flask.current_app.logger.debug('Successfully scheduled request %d', request.id)
    return flask.jsonify(request.to_json()), 201


@api_v1.route('/builds/regenerate-bundle-batch', methods=['POST'])
@login_required
def regenerate_bundle_batch():
    """
    Submit a batch of requests to regenerate operator bundle images.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload = flask.request.get_json()
    Batch.validate_batch_request_params(payload)

    batch = Batch(annotations=payload.get('annotations'))
    db.session.add(batch)

    requests = []
    # Iterate through all the build requests and verify that the requests are valid before
    # committing them and scheduling the tasks
    for build_request in payload['build_requests']:
        try:
            request = RequestRegenerateBundle.from_json(build_request, batch)
        except ValidationError as e:
            # Rollback the transaction if any of the build requests are invalid
            db.session.rollback()
            raise ValidationError(
                f'{str(e).rstrip(".")}. This occurred on the build request in '
                f'index {payload["build_requests"].index(build_request)}.'
            )
        db.session.add(request)
        requests.append(request)

    db.session.commit()
    messaging.send_messages_for_new_batch_of_requests(requests)

    request_jsons = []
    # This list will be used for the log message below and avoids the need of having to iterate
    # through the list of requests another time
    processed_request_ids = []
    build_and_requests = zip(payload['build_requests'], requests)
    try:
        for build_request, request in build_and_requests:
            error_callback = failed_request_callback.s(request.id)
            handle_regenerate_bundle_request.apply_async(
                args=[
                    build_request['from_bundle_image'],
                    build_request.get('organization'),
                    request.id,
                ],
                link_error=error_callback,
                queue=_get_user_queue(),
            )

            request_jsons.append(request.to_json())
            processed_request_ids.append(str(request.id))
    except kombu.exceptions.OperationalError:
        unprocessed_requests = [r for r in requests if str(r.id) not in processed_request_ids]
        handle_broker_batch_error(unprocessed_requests)

    flask.current_app.logger.debug(
        'Successfully scheduled the batch %d with requests: %s',
        batch.id,
        ', '.join(processed_request_ids),
    )
    return flask.jsonify(request_jsons), 201


@api_v1.route('/builds/add-rm-batch', methods=['POST'])
@login_required
def add_rm_batch():
    """
    Submit a batch of requests to add or remove operators from an index image.

    :rtype: flask.Response
    :raise ValidationError: if required parameters are not supplied
    """
    payload = flask.request.get_json()
    Batch.validate_batch_request_params(payload)

    batch = Batch(annotations=payload.get('annotations'))
    db.session.add(batch)

    requests = []
    # Iterate through all the build requests and verify that the requests are valid before
    # committing them and scheduling the tasks
    for build_request in payload['build_requests']:
        try:
            if build_request.get('operators'):
                # Check for the validity of a RM request
                request = RequestRm.from_json(build_request, batch)
            elif build_request.get('bundles'):
                # Check for the validity of an Add request
                request = RequestAdd.from_json(build_request, batch)
            else:
                raise ValidationError('Build request is not a valid Add/Rm request.')
        except ValidationError as e:
            raise ValidationError(
                f'{str(e).rstrip(".")}. This occurred on the build request in '
                f'index {payload["build_requests"].index(build_request)}.'
            )
        db.session.add(request)
        requests.append(request)

    db.session.commit()
    messaging.send_messages_for_new_batch_of_requests(requests)

    request_jsons = []
    # This list will be used for the log message below and avoids the need of having to iterate
    # through the list of requests another time
    processed_request_ids = []
    for build_request, request in zip(payload['build_requests'], requests):
        request_jsons.append(request.to_json())

        overwrite_from_index = _should_force_overwrite() or build_request.get(
            'overwrite_from_index'
        )
        celery_queue = _get_user_queue(serial=overwrite_from_index)
        if isinstance(request, RequestAdd):
            args = [
                build_request['bundles'],
                build_request['binary_image'],
                request.id,
                build_request.get('from_index'),
                build_request.get('add_arches'),
                build_request.get('cnr_token'),
                build_request.get('organization'),
                build_request.get('force_backport'),
                overwrite_from_index,
                build_request.get('overwrite_from_index_token'),
                flask.current_app.config['IIB_GREENWAVE_CONFIG'].get(celery_queue),
            ]
        elif isinstance(request, RequestRm):
            args = [
                build_request['operators'],
                build_request['binary_image'],
                request.id,
                build_request['from_index'],
                build_request.get('add_arches'),
                overwrite_from_index,
                build_request.get('overwrite_from_index_token'),
            ]

        safe_args = copy.copy(args)
        if build_request.get('cnr_token'):
            safe_args[safe_args.index(build_request['cnr_token'])] = '*****'
        if build_request.get('overwrite_from_index_token'):
            safe_args[safe_args.index(build_request['overwrite_from_index_token'])] = '*****'

        error_callback = failed_request_callback.s(request.id)
        try:
            if isinstance(request, RequestAdd):
                handle_add_request.apply_async(
                    args=args,
                    link_error=error_callback,
                    argsrepr=repr(safe_args),
                    queue=celery_queue,
                )
            else:
                handle_rm_request.apply_async(
                    args=args,
                    link_error=error_callback,
                    argsrepr=repr(safe_args),
                    queue=celery_queue,
                )
        except kombu.exceptions.OperationalError:
            unprocessed_requests = [r for r in requests if str(r.id) not in processed_request_ids]
            handle_broker_batch_error(unprocessed_requests)

        processed_request_ids.append(str(request.id))

    flask.current_app.logger.debug(
        'Successfully scheduled the batch %d with requests: %s',
        batch.id,
        ', '.join(processed_request_ids),
    )
    return flask.jsonify(request_jsons), 201
