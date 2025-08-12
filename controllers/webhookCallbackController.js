import sqldb from "../mysqldb.js";
import { graphqlClient } from "../helpers/commonFnHelper.js";
import { httpStatusCodes } from "./errorController.js";

import {
  emitTagEvent,
  emitCollectionCreateEvent,
  emitCollectionUpdateEvent,
  emitCollectionDeleteEvent,
} from "../services/socketEvents.js";

// Define GraphQL queries outside of the route
const GET_META_OBJECT_DEFINITION = `query getMetaObjects($after: String) {
  metaobjectDefinition(id: "gid://shopify/MetaobjectDefinition/5286887447") {
    metaobjects(first: 250, after: $after) {
      pageInfo {
        hasNextPage
        endCursor
      }
      edges {
        node {
          displayName
        }
      }
    }
  }
}`;

const CREATE_META_OBJECT = `mutation CreateMetaobject($metaobject: MetaobjectCreateInput!) {
  metaobjectCreate(metaobject: $metaobject) {
    metaobject {
      handle
      season: field(key: "season") {
        value
      }
    }
    userErrors {
      field
      message
      code
    }
  }
}`;

const PRODUCTS_QUERY = `
  query getCollectionProducts($id: ID!, $cursor: String) {
    collection(id: $id) {
      products(first: 100, after: $cursor) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          tags
        }
      }
    }
  }
`;

const COLLECTIONS_QUERY = `
  query getCollections($cursor: String) {
    collections(first: 50, after: $cursor) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        handle
        title
        id
      }
    }
  }
`;

// Webhook url which creates metaobject based on tags
export const createMetaobjectCallback = async (req, res) => {
  const tagUrls = [];
  let hasNextCollectionPage = true;
  let endCursorCollection = null;
  let tagsWithTagUrls = {};

  while (hasNextCollectionPage) {
    const collectionsRes = await graphqlClient(COLLECTIONS_QUERY, {
      cursor: endCursorCollection,
    });
    const collections = collectionsRes.data.collections;
    for (const collection of collections.nodes) {
      const uniqueTags = new Set();
      let hasNextProductPage = true;
      let endCursorProduct = null;

      while (hasNextProductPage) {
        const res = await graphqlClient(PRODUCTS_QUERY, {
          id: collection.id,
          cursor: endCursorProduct,
        });

        const products = res.data.collection?.products?.nodes ?? [];
        const pageInfo = res.data.collection?.products?.pageInfo;

        products.forEach((product) => {
          product.tags.forEach((tag) => uniqueTags.add(tag));
        });

        hasNextProductPage = pageInfo?.hasNextPage;
        endCursorProduct = pageInfo?.endCursor;
      }

      Array.from(uniqueTags).forEach((tag) => {
        const slug = tag
          .toLowerCase()
          .replace(/\s+/g, "-")
          .replace(/[^\w-]/g, "");

        tagUrls.push(`${collection.handle}/${slug}`);
        tagsWithTagUrls[`${collection.handle}/${slug}`] = tag;
      });
    }

    hasNextCollectionPage = collections.pageInfo.hasNextPage;
    endCursorCollection = collections.pageInfo.endCursor;
  }

  const displayNameSet = new Set();
  let hasNextPage = true;
  let afterCursor = null;

  while (hasNextPage) {
    const response = await graphqlClient(GET_META_OBJECT_DEFINITION, {
      after: afterCursor,
    });

    const metaobjects = response.data.metaobjectDefinition.metaobjects;

    metaobjects.edges.forEach((edge) => {
      if (edge.node.displayName) {
        displayNameSet.add(edge.node.displayName);
      }
    });

    hasNextPage = metaobjects.pageInfo.hasNextPage;
    afterCursor = metaobjects.pageInfo.endCursor;
  }

  for (const tag of tagUrls) {
    // insert the "new meta object" into the meta objects list
    if (!displayNameSet.has(tag)) {
      await graphqlClient(CREATE_META_OBJECT, {
        metaobject: {
          type: "collection_style",
          capabilities: { publishable: { status: "ACTIVE" } },
          fields: [
            {
              key: "style_url",
              value: tag,
            },
            {
              key: "tag",
              value: tagsWithTagUrls[tag],
            },
            {
              key: "title",
              value: tag.replace("/", "-"),
            },
          ],
        },
      });
    }
  }

  // insert the "new tag" into the stlr_tags table
  for (const tag of Object.keys(tagsWithTagUrls)) {
    const tagName = tagsWithTagUrls[tag];

    const [rows] = await sqldb.query(
      "SELECT * FROM stlr_tags WHERE tag_name = ?",
      [tagName]
    );

    if (rows.length === 0) {
      await sqldb.query("INSERT INTO stlr_tags (tag_name) VALUES (?)", [
        tagName,
      ]);

      // Emit notification for new tag
      emitTagEvent(tagName);
    }
  }

  return res.status(200).json({
    status: 200,
    success: true,
    message: "OK",
    data: tagUrls,
    tagsWithTagUrls,
  });
};

// Callback for create collection
export const createCollectionCallback = async (req, res) => {
  const connection = await sqldb.getConnection();

  const collectionData = req.body;

  const categoryData = {
    name: collectionData.title,
    slug: collectionData.handle,
    shopify_id: collectionData.admin_graphql_api_id,
  };

  try {
    await connection.beginTransaction();

    // Check if the category already exists
    const [existingRows] = await connection.execute(
      "SELECT * FROM stlr_categories WHERE shopify_id = ?",
      [categoryData.shopify_id]
    );

    if (existingRows.length > 0) {
      await connection.commit(); // No changes made
      return res.status(200).json({
        status: 200,
        success: false,
        message: "Category already exists.",
        data: existingRows[0],
      });
    }

    // Insert new category
    const [result] = await connection.execute(
      "INSERT INTO stlr_categories (name, slug, shopify_id) VALUES (?, ?, ?)",
      [categoryData.name, categoryData.slug, categoryData.shopify_id]
    );

    const newCategory = {
      category_id: result.insertId,
      ...categoryData,
    };

    await connection.commit();

    console.log("New category inserted:", newCategory);

    // send event to the browser
    emitCollectionCreateEvent(newCategory);

    return res.status(201).json({
      status: 201,
      success: true,
      message: "Category created successfully.",
      data: newCategory,
    });
  } catch (error) {
    await connection.rollback();
    console.error("Error inserting category:", error);
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Error inserting category.",
      error: error.message,
    });
  } finally {
    connection.release();
  }
};

// Callback for update collection
export const updateCollectionCallback = async (req, res) => {
  const connection = await sqldb.getConnection();
  const collectionData = req.body;

  const shopifyId = collectionData.admin_graphql_api_id;
  const updatedName = collectionData.title;

  try {
    await connection.beginTransaction();

    // Fetch the existing category based on shopify_id
    const [existingRows] = await connection.execute(
      "SELECT * FROM stlr_categories WHERE shopify_id = ?",
      [shopifyId]
    );

    if (existingRows.length === 0) {
      await connection.commit(); // nothing to update
      return res.status(404).json({
        status: 404,
        success: false,
        message: "Category not found for the given Shopify ID.",
      });
    }

    const existingCategory = existingRows[0];

    // Compare names - only update if the name has changed
    if (existingCategory.name !== updatedName) {
      await connection.execute(
        "UPDATE stlr_categories SET name = ? WHERE shopify_id = ?",
        [updatedName, shopifyId]
      );

      await connection.commit();

      const updatedCategory = {
        category_id: existingCategory.category_id,
        name: updatedName,
        slug: existingCategory.slug, // assuming slug doesn't change
        shopify_id: shopifyId,
      };

      // Emit update event to browser
      emitCollectionUpdateEvent(updatedCategory);

      return res.status(200).json({
        status: 200,
        success: true,
        message: "Category name updated successfully.",
        data: updatedCategory,
      });
    } else {
      // Name didn't change — no update needed
      await connection.commit();

      return res.status(200).json({
        status: 200,
        success: false,
        message: "No changes detected. Category name is unchanged.",
      });
    }
  } catch (error) {
    await connection.rollback();
    console.error("Error updating category:", error);
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Error updating category.",
      error: error.message,
    });
  } finally {
    connection.release();
  }
};

// Callback for delete collection
export const deleteCollectionCallback = async (req, res) => {
  const connection = await sqldb.getConnection();
  const collectionData = req.body;

  const numericId = collectionData.id;
  const shopifyId = `gid://shopify/Collection/${numericId}`;

  try {
    await connection.beginTransaction();

    // Get the category_id from the shopify_id
    const [categoryRows] = await connection.execute(
      "SELECT category_id FROM stlr_categories WHERE shopify_id = ?",
      [shopifyId]
    );

    if (categoryRows.length === 0) {
      return res.status(404).json({
        status: 404,
        success: false,
        message: "Category not found. Nothing to delete.",
      });
    }

    const categoryId = categoryRows[0].category_id;

    // Step 1: Set category_id = 0 in stlr_rings where category_id matches
    await connection.execute(
      "UPDATE stlr_rings SET category_id = 0 WHERE category_id = ?",
      [categoryId]
    );

    // Step 2: Delete the category from stlr_categories
    await connection.execute(
      "DELETE FROM stlr_categories WHERE category_id = ?",
      [categoryId]
    );

    await connection.commit();

    // Emit deletion event
    emitCollectionDeleteEvent({
      category_id: categoryId,
      shopify_id: shopifyId,
    });

    return res.status(200).json({
      status: 200,
      success: true,
      message: "Category deleted successfully.",
      data: {
        category_id: categoryId,
        shopify_id: shopifyId,
      },
    });
  } catch (error) {
    await connection.rollback();
    console.error("Error deleting category:", error);
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal server error",
      error: error.message || error,
    });
  } finally {
    connection.release();
  }
};

// Callback for metafield definition update; This is valid only when the user does the "single operation"; This function is not valid if the user does "multiple operation" on metafield definition.
export const metafieldDefinitionUpdateCallback = async (req, res) => {
  const connection = await sqldb.getConnection();

  try {
    // Get the new list from the Shopify
    const options = req.body.options || [];
    const choicesOption = options.find((opt) => opt.name === "choices");
    const newList = choicesOption?.value ? JSON.parse(choicesOption.value) : [];

    // Get the old list from the DB
    const [rows] = await connection.execute(
      `SELECT group_name FROM wedding_band.stlr_groups`
    );
    const oldList = rows.map((row) => row.group_name);

    // Compare lists
    const createdItems = newList.filter((item) => !oldList.includes(item));
    const deletedItems = oldList.filter((item) => !newList.includes(item));

    /**
     * Determine whether the item is created, updated or deleted
     */
    if (createdItems.length === 1 && deletedItems.length === 1) {
      // Rename: One item replaced with another
      const renamedFrom = deletedItems[0];
      const renamedTo = createdItems[0];
      console.log(`Renamed "${renamedFrom}" to "${renamedTo}"`);

      // → Your rename logic here
      await connection.execute(
        `UPDATE wedding_band.stlr_groups SET group_name = ? WHERE group_name = ?`,
        [renamedTo, renamedFrom]
      );
    } else if (createdItems.length > 0) {
      // Created items
      console.log("Created Items:", createdItems);
      for (const item of createdItems) {
        await connection.execute(
          `INSERT INTO wedding_band.stlr_groups (group_name) VALUES (?)`,
          [item]
        );
      }
    } else if (deletedItems.length > 0) {
      // Deleted items
      console.log("Deleted Items:", deletedItems);
      for (const item of deletedItems) {
        await connection.execute(
          `DELETE FROM wedding_band.stlr_groups WHERE group_name = ?`,
          [item]
        );
      }
    } else {
      console.log("Unhandled change pattern");
    }

    return res.status(201).json({
      status: 201,
      success: true,
      message: "Category created successfully.",
      data: "hello",
    });
  } catch (error) {
    console.error("Error creating group:", error);
    return res.status(500).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: "Internal Server Error",
    });
  } finally {
    if (connection) connection.release();
  }
};

// Callback for customer creation;
export const createCustomerCallback = async (req, res) => {
  const connection = await sqldb.getConnection();

  try {
    const customer = req.body;

    // Destructure necessary fields
    const {
      id: customerId,
      email,
      first_name = null,
      last_name = null,
      phone = null,
    } = customer;

    const [existing] = await connection.execute(
      "SELECT * FROM customers WHERE email = ?",
      [email]
    );

    if (existing.length > 0) {
      return res.status(200).json({
        status: 200,
        success: true,
        message: "Customer already exists. Skipped insertion.",
      });
    }

    // Insert customer
    await connection.execute(
      `INSERT INTO customers 
        (customerId, email, first_name, last_name, phone)
       VALUES (?, ?, ?, ?, ?)`,
      [customerId, email, first_name, last_name, phone]
    );

    return res.status(201).json({
      status: 201,
      success: true,
      message: "Customer created successfully.",
    });
  } catch (error) {
    console.error("Error creating group:", error);
    return res.status(500).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: "Internal Server Error",
    });
  } finally {
    if (connection) connection.release();
  }
};

// ------------------*---------------------*----------------------*-----------------------*---------------------------*----------------------*--------------------*---------------------------------

// Subscribe for webhook api
export const subscribeWebhook = async (req, res) => {
  try {
    // Define the GraphQL mutation to subscribe to the webhook
    const mutation = `
      mutation {
        webhookSubscriptionCreate(
          topic: METAFIELD_DEFINITIONS_UPDATE,
          webhookSubscription: {
            callbackUrl: "https://wbsbk.chicago-jewelers.com/api/v1/metafield-definition-update-callback", 
            format: JSON
          }
        ) {
          webhookSubscription {
            id
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    // Execute the GraphQL mutation using your graphqlClient
    const response = await graphqlClient(mutation);

    // Handle the response from the mutation
    if (response.data.webhookSubscriptionCreate.userErrors.length > 0) {
      return res.status(400).json({
        status: 400,
        success: false,
        message: "Error subscribing to webhook",
        errors: response.data.webhookSubscriptionCreate.userErrors,
      });
    }

    // Return the webhook subscription ID on success
    return res.status(200).json({
      status: 200,
      success: true,
      message: "Webhook subscription created successfully",
      webhookId: response.data.webhookSubscriptionCreate.webhookSubscription.id,
    });
  } catch (error) {
    console.error("Error subscribing to webhook:", error);
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal server error",
      error: error.message || error,
    });
  }
};

// see all the total webhooks
export const listAllSubscribedWebhooks = async (req, res) => {
  try {
    const query = `
      {
        webhookSubscriptions(first: 10) {
          edges {
            node {
              id
              topic
              endpoint {
                ... on WebhookHttpEndpoint {
                  callbackUrl
                }
              }
            }
          }
        }
      }
    `;

    // Assuming graphqlClient is a function that sends the query and gets the response
    const response = await graphqlClient(query);

    // Send the response back to the client
    return res.json(response);
  } catch (error) {
    console.error("Error fetching webhooks:", error);
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal server error",
      error: error.message || error,
    });
  }
};

// Unsubscribe from webhook API
export const unsubscribeWebhook = async (req, res) => {
  try {
    const { webhookId } = req.body;

    if (!webhookId) {
      return res.status(400).json({
        status: 400,
        success: false,
        message: "Missing 'webhookId' in request body",
      });
    }

    // Define the GraphQL mutation to delete the webhook
    const mutation = `
      mutation webhookSubscriptionDelete($id: ID!) {
        webhookSubscriptionDelete(id: $id) {
          deletedWebhookSubscriptionId
          userErrors {
            field
            message
          }
        }
      }
    `;

    const variables = {
      id: webhookId,
    };

    // Execute the GraphQL mutation using your graphqlClient
    const response = await graphqlClient(mutation, variables);

    // Check for errors
    const errors = response.data.webhookSubscriptionDelete.userErrors;
    if (errors && errors.length > 0) {
      return res.status(400).json({
        status: 400,
        success: false,
        message: "Error unsubscribing from webhook",
        errors: errors,
      });
    }

    // Success response
    return res.status(200).json({
      status: 200,
      success: true,
      message: "Webhook unsubscribed successfully",
      deletedWebhookId:
        response.data.webhookSubscriptionDelete.deletedWebhookSubscriptionId,
    });
  } catch (error) {
    console.error("Error unsubscribing from webhook:", error);
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal server error",
      error: error.message || error,
    });
  }
};
