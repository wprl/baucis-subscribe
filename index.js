// __Dependencies__
var events = require('baucis-events');

// __Module Definition__
var plugin = module.exports = function () {
  var baucis = this;

  // __Private Module Members__
  var channel = baucis.channel();
  // Method to create a listener to listen to the channel.  The callback
  // emits events or errors.
  function createListener (tag, context, callback) { // TODO move to controller.listen()
    var controller = context.controller;
    var findBy = controller.findBy();
    // Listen for events to send to the remote requester.
    var subscription = channel.subscribe(tag, function (e) {
      // Don't process events if the event type is disabled.
      if (!controller.events(tag)) return;
      // If the document ID doesn't match the event ID, bail.
      if (context.doc[findBy] !== e.doc[findBy]) return;
      // Otherwise, emit the event to the outgoing stream.
      callback(null, e);
    });
    response.on('end', function () {
      subscription.unsubscribe();
    });
  }
  // Create the formatter.
  function events (response) {
    // The formatter is a pipeline of through streams.
    var pipeline = protect.pipeline();
    // These probably don't have much affect...
    response.set('Cache-Control', 'no-cache');
    response.set('Connection': 'keep-alive');
    // Check that only documents are being emitted.
    pipeline(function (context, callback) {
      if (typeof context.doc !== 'object') {
        callback(baucis.Error.BadRequest("You can't listen for events with queries that do not return documents"));
        return;
      }
      callback(null, context);
    });
    // Create listeners for the documents that match the query.  Swallow query
    // documents, and emit events.  The outgoing stream in this context is the
    // `text/event-stream` response that is kept alive for SSE.
    pipeline(function (context, callback) {
      createListener('created', context, callback);
      createListener('updated', context, callback);
      createListener('deleted', context, callback);
    });
    // Map each emitted event to an SSE formatted event.
    var formatter = pipeline(function (e, callback) {
      var id = new Date();
      var remoteTag = [ 'baucis', controller.singular(), e.tag ].join('.');
      response.write('id: ' + id);
      response.write('\n');
      response.write('event: ' + remoteTag);
      response.write('\n');
      response.write('data: ' + JSON.stringify(e));
      response.write('\n');
    });

    var events = pipeline();
    events.pause();

    // Respect `Last-Event-ID` and send any missed documents if requested.
    if (lastEventId) {
      var query = ...;
      // Find the missed events.  The oldest will be sent first.
      query.stream().pipe(formatter);
    }
    // Send the current state of the object, avoiding the need for two requests.
    else {
      currentQuery.stream().pipe(es.map(function (doc, callback) {
        callback(null, { tag: 'state', doc: doc });
      })).pipe(formatter);
    }

    events.pipe(formatter);
    events.resume();

    return events;
  }

  // __External Decorators__
  // Add the `text/event-stream` formatter to the API.
  baucis.Api.decorators(function () {
    this.setFormatter('text/event-stream', events());
  });
};
