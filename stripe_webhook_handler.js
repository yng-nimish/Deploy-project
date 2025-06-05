// Standard AWS Lambda handler
exports.handler = async (event) => {
  // 1. Import the stripe library
  // Ensure you have 'stripe' in your package.json and it's installed.
  // Initialize Stripe with your API secret key.
  // IMPORTANT: This is your STRIPE API SECRET KEY (e.g., sk_test_.... or sk_live_....),
  // NOT your webhook signing secret.
  // It should be stored as an environment variable for security.
  if (!process.env.STRIPE_SECRET_KEY) {
    console.error('Stripe API secret key is not configured. Set STRIPE_SECRET_KEY environment variable.');
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Stripe API secret key not configured' }),
    };
  }
  const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);

  // 2. Retrieve the webhook endpoint secret from environment variables
  // This is the secret you get from the Stripe dashboard when creating a webhook endpoint (e.g., whsec_...).
  // It should be stored as an environment variable (e.g., STRIPE_WEBHOOK_SECRET_TEST).
  const endpointSecret = process.env.STRIPE_WEBHOOK_SECRET_TEST;

  if (!endpointSecret) {
    console.error('Stripe webhook secret is not configured. Set STRIPE_WEBHOOK_SECRET_TEST environment variable.');
    // This matches the original error the user was asking about.
    return {
      statusCode: 500, // Internal Server Error, as the server is misconfigured
      body: JSON.stringify({ error: 'Webhook secret not configured' }),
    };
  }

  // 3. Get the Stripe-Signature header
  // The signature is included in the 'Stripe-Signature' header of the request.
  // Header names can be case-insensitive, so check common variations.
  const sig = event.headers['stripe-signature'] || event.headers['Stripe-Signature'];

  if (!sig) {
    console.error('Missing Stripe-Signature header.');
    return {
      statusCode: 400,
      body: JSON.stringify({ error: 'Missing Stripe-Signature header' }),
    };
  }

  // 4. Get the raw request body
  // IMPORTANT: Stripe requires the raw request body to construct and verify the event.
  // If you are using API Gateway with Lambda proxy integration, the raw body is typically available at `event.body`.
  // Ensure that no middleware (like Express's `express.json()` or `bodyParser.json()`)
  // has parsed the body before it reaches this point. If it has, `stripe.webhooks.constructEvent` will fail.
  const rawBody = event.body;

  if (!rawBody) {
    console.error('Missing raw request body.');
    return {
      statusCode: 400,
      body: JSON.stringify({ error: 'Missing raw request body' }),
    };
  }

  console.log('Event received. Validating Stripe signature...');

  let stripeEvent;

  try {
    // 5. Verify the webhook signature and construct the event
    stripeEvent = stripe.webhooks.constructEvent(rawBody, sig, endpointSecret);
    console.log(`Stripe event constructed successfully: ${stripeEvent.id}, type: ${stripeEvent.type}`);
  } catch (err) {
    // If `constructEvent` throws an error, the signature is invalid or there's another issue.
    console.error(`Webhook signature verification failed: ${err.message}`);
    return {
      statusCode: 400, // Bad Request
      body: JSON.stringify({ error: `Webhook error: ${err.message}` }),
    };
  }

  // 6. Handle the event (e.g., a `switch` statement based on `stripeEvent.type`)
  switch (stripeEvent.type) {
    case 'checkout.session.completed':
      const session = stripeEvent.data.object;
      // This is where your business logic goes.
      // For example, provision a service, send a confirmation email, update your database.
      console.log(`Successfully processed checkout.session.completed for session ${session.id}`);
      console.log('Customer email:', session.customer_details ? session.customer_details.email : 'N/A (customer_details not expanded or available)');
      console.log('Payment status:', session.payment_status);
      // Add your business logic here (e.g., fulfill order, grant access)
      break;
    // Add other event types you want to handle
    // case 'payment_intent.succeeded':
    //   const paymentIntent = stripeEvent.data.object;
    //   console.log(`Successfully processed payment_intent.succeeded for PaymentIntent ${paymentIntent.id}`);
    //   // Add your business logic here
    //   break;
    default:
      console.warn(`Unhandled event type: ${stripeEvent.type}`);
  }

  // 7. Return a 200 OK response to Stripe to acknowledge receipt of the event
  // If Stripe does not receive a 2xx response, it will continue to retry sending the webhook.
  return {
    statusCode: 200,
    body: JSON.stringify({ received: true, eventId: stripeEvent.id }),
  };
};
