import sqldb from "../mysqldb.js";
import cron from "node-cron";
import { graphqlClient } from "../helpers/commonFnHelper.js";

const DELETE_META_OBJECT = `
  mutation DeleteMetaobject($id: ID!) {
    metaobjectDelete(id: $id) {
      deletedId
      userErrors {
        field
        message
      }
    }
  }
`;

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

const GET_META_OBJECT_DEFINITION = `
  query getMetaObjects($after: String) {
    metaobjectDefinition(id: "gid://shopify/MetaobjectDefinition/5286887447") {
      metaobjects(first: 250, after: $after) {
        pageInfo {
          hasNextPage
          endCursor
        }
        edges {
          node {
            id
            displayName
          }
        }
      }
    }
  }
`;

// Delete rows where the token has expired;
const removeExpiredTokens = async () => {
  const connection = await sqldb.getConnection();
  try {
    console.log("Running cron job to remove expired reset password tokens");

    // Start a transaction
    await connection.beginTransaction();

    const currentTime = new Date();
    const query = `
      DELETE FROM reset_passwords
      WHERE expires_at < ?;
    `;

    await connection.execute(query, [currentTime]);

    // Commit the transaction
    await connection.commit();

    console.log("Expired reset password tokens removed successfully");
  } catch (error) {
    // Rollback the transaction in case of error
    await connection.rollback();
    console.error(
      "Error while removing expired tokens:",
      error.message || error
    );
  } finally {
    connection.release(); // Release connection back to pool
  }
};

// Second Cron Job:- Remove unwanted stuller groups which dont have any products
const removeJunkStullerGroups = async () => {
  const connection = await sqldb.getConnection();
  try {
    console.log("Running cron job to remove junk groups");

    await connection.beginTransaction();

    const [allRings] = await connection.query("SELECT ring_id FROM stlr_rings");

    const [variationCounts] = await connection.query(`
      SELECT ring_id, COUNT(*) as variationCount
      FROM stlr_ring_variations
      GROUP BY ring_id
    `);

    const variationMap = new Map();
    variationCounts.forEach(({ ring_id, variationCount }) => {
      variationMap.set(ring_id, variationCount);
    });

    for (const { ring_id } of allRings) {
      const count = variationMap.get(ring_id) || 0;

      if (count === 0) {
        const [[{ categoryCount }]] = await connection.query(
          "SELECT COUNT(*) as categoryCount FROM stlr_ring_has_categories WHERE ring_id = ?",
          [ring_id]
        );

        if (categoryCount > 0) {
          await connection.query(
            "DELETE FROM stlr_ring_has_categories WHERE ring_id = ?",
            [ring_id]
          );
        }

        await connection.query("DELETE FROM stlr_rings WHERE ring_id = ?", [
          ring_id,
        ]);

        console.log(`Deleted ring ${ring_id} and its related categories`);
      }
    }

    await connection.commit();
    console.log("Junk groups removed successfully");
  } catch (error) {
    await connection.rollback();
    console.error("Error while removing junk groups:", error.message || error);
  } finally {
    connection.release();
  }
};

/*
// Third Cron Job:- Remove non-associated meta objects. This API will remove the meta objects which are not linked with any products
const removeJunkMetaObjects = async () => {
  try {
    console.log("Running cron job to remove junk meta objects");

    const tagUrls = [];
    let hasNextCollectionPage = true;
    let endCursorCollection = null;

    // Step 1: Generate tagUrls from collections and products
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
        });
      }

      hasNextCollectionPage = collections.pageInfo.hasNextPage;
      endCursorCollection = collections.pageInfo.endCursor;
    }

    // Step 2: Fetch existing metaobjects
    const displayNameToIdMap = new Map();
    let hasNextPage = true;
    let afterCursor = null;

    while (hasNextPage) {
      const response = await graphqlClient(GET_META_OBJECT_DEFINITION, {
        after: afterCursor,
      });

      const metaobjects = response.data.metaobjectDefinition.metaobjects;

      metaobjects.edges.forEach((edge) => {
        if (edge.node.displayName && edge.node.id) {
          displayNameToIdMap.set(edge.node.displayName, edge.node.id);
        }
      });

      hasNextPage = metaobjects.pageInfo.hasNextPage;
      afterCursor = metaobjects.pageInfo.endCursor;
    }

    // Step 3: Delete junk metaobjects (not present in tagUrls)
    for (const [displayName, id] of displayNameToIdMap) {
      if (!tagUrls.includes(displayName)) {
        await graphqlClient(DELETE_META_OBJECT, { id });
      }
    }

    console.log("Junk groups removed successfully");
  } catch (error) {
    console.error("Error while removing junk groups:", error.message || error);
  }
};
*/

// Schedule the task to run daily at 8:00 AM
cron.schedule("0 8 * * *", async () => {
  await removeExpiredTokens();
  await removeJunkStullerGroups();
  // await removeJunkMetaObjects();
});
