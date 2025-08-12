import { httpStatusCodes, tryCatchFn } from "./errorController.js";
import sqldb from "../mysqldb.js";
import { graphqlClient } from "../helpers/commonFnHelper.js";
import Joi from "joi";

// const ADD_TAG_TO_PRODUCT = `
// mutation addTags($id: ID!, $tags: [String!]!) {
//   tagsAdd(id: $id, tags: $tags) {
//     node {
//       id
//     }
//     userErrors {
//       message
//     }
//   }
// }`;

// const REMOVE_TAG_FROM_PRODUCT = `
// mutation removeTags($id: ID!, $tags: [String!]!) {
//   tagsRemove(id: $id, tags: $tags) {
//     node {
//       id
//     }
//     userErrors {
//       message
//     }
//   }
// }`;

const getGroup = tryCatchFn(async (req, res) => {
  try {
    // Define Joi schema for request body validation
    const schema = Joi.object({
      search: Joi.string().allow("").optional(),
      sort: Joi.string().valid("ASC", "DESC").default("ASC"),
      page: Joi.number().integer().min(1).default(1),
      limit: Joi.number().integer().min(1).max(100).default(20),
    });

    // Validate request body
    const { error, value } = schema.validate(req.body);

    if (error) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: error.details[0].message,
      });
    }

    const { search, sort, page, limit } = value;
    const offset = (page - 1) * limit;

    // Prepare search condition
    let whereClause = "";
    const params = [];
    if (search) {
      whereClause = " WHERE g.group_name LIKE ? ";
      params.push(`%${search}%`);
    }

    // Fetch paginated groups
    const query = `
            SELECT 
                g.*
            FROM stlr_groups g
            ${whereClause}
            ORDER BY g.group_name ${sort}
            LIMIT ? OFFSET ?;
        `;

    params.push(limit, offset);
    const [groups] = await sqldb.query(query, params);

    // Fetch total count for pagination
    const countQuery = `SELECT COUNT(*) AS total FROM stlr_groups g ${whereClause}`;
    const countParams = search ? [`%${search}%`] : [];
    const [[countResult]] = await sqldb.query(countQuery, countParams);

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Groups fetched successfully.",
      result: groups,
      pagination: {
        currentPage: page,
        totalPages: Math.ceil(countResult.total / limit),
        totalRecords: countResult.total,
      },
    });
  } catch (error) {
    console.error("Error fetching groups:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
});

const getGroupsByIds = tryCatchFn(async (req, res) => {
  const schema = Joi.object({
    group_ids: Joi.array()
      .items(Joi.number().integer().positive().required())
      .min(1)
      .required()
      .messages({
        "array.base": "group_ids must be an array.",
        "array.min": "At least one group_id must be provided.",
        "any.required": "group_ids array is required.",
      }),
  });

  const { error } = schema.validate(req.body);
  if (error) {
    return res.status(400).json({
      status: 400,
      success: false,
      message: error.details[0].message,
    });
  }

  const { group_ids } = req.body;

  const connection = await sqldb.getConnection();
  try {
    const [groups] = await connection.execute(
      `SELECT * FROM wedding_band.stlr_groups 
             WHERE group_id IN (${group_ids.map(() => "?").join(", ")})`,
      group_ids
    );

    // if (groups.length === 0) {
    //     return res.status(404).json({
    //         status: 404,
    //         success: false,
    //         message: "No groups found for the provided IDs."
    //     });
    // }

    return res.status(200).json({
      status: 200,
      success: true,
      message: "Groups fetched successfully.",
      result: groups,
    });
  } catch (error) {
    console.error("Error fetching groups:", error);
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal Server Error",
    });
  } finally {
    connection.release();
  }
});

// Create a group
const createGroup = tryCatchFn(async (req, res) => {
  // Joi schema for group_name validation
  const groupSchema = Joi.object({
    group_name: Joi.string()
      .trim()
      .min(2)
      .max(100)
      .regex(/^(?!\d+$).*$/) // Prevents purely numeric names
      .required()
      .messages({
        "string.empty": "Group name is required.",
        "string.min": "Group name must be at least 2 characters long.",
        "string.max": "Group name cannot exceed 100 characters.",
        "string.pattern.base": "Group name cannot be only numbers.",
      }),
  });

  const { error } = groupSchema.validate(req.body);
  if (error) {
    return res.status(400).json({
      status: httpStatusCodes.NOT_FOUND,
      success: false,
      message: error.details[0].message,
    });
  }

  const { group_name } = req.body;

  if (!group_name) {
    return res.status(400).json({
      status: httpStatusCodes.NOT_FOUND,
      success: false,
      message: "Group name is required",
    });
  }

  // Create a database connection
  const connection = await sqldb.getConnection(); // Get connection from the pool

  try {
    // Begin the transaction
    await connection.beginTransaction();

    // Check if the group name already exists
    const [existingGroup] = await connection.execute(
      `SELECT group_id FROM wedding_band.stlr_groups WHERE group_name = ?`,
      [group_name]
    );

    if (existingGroup.length > 0) {
      return res.status(409).json({
        status: 409,
        success: false,
        message: "Group name already exists",
      }); // 409 Conflict
    }

    /**
     * Insert the new group into the metafield "Group Name" predefined list
     */
    // STEP 1: Get the metafields for a product and get the id of the field "Group Name"
    const METAFIELD_DEFINITIONS_QUERY = `
      query {
        metafieldDefinitions(first: 100, ownerType: PRODUCT) {
          edges {
            node {
              id
              name
              namespace
              key
              validations {
                name
                value
              }
            }
          }
        }
      }
    `;

    const response1 = await graphqlClient(METAFIELD_DEFINITIONS_QUERY);

    const definitions = response1?.data?.metafieldDefinitions?.edges?.map(
      (edge) => edge.node
    );

    const groupNameMetafield = definitions.find(
      (def) => def.name === "Group Name"
    );

    // Check if the "Group Name" metafield is found
    if (!groupNameMetafield) {
      throw new Error("Group Name metafield definition not found");
    }

    // Extract the ID of the "Group Name" metafield
    const groupNameMetafieldId = groupNameMetafield.id;

    // Step 2: Fetch predefined values related to the "Group Name" metafield (using its ID)
    const METAFIELD_PREDEFINED_VALUES_QUERY = `
      query {
        metafieldDefinition(id: "${groupNameMetafieldId}") {
          id
          name
          namespace
          key
          validations {
            name
            value
          }
        }
      }
    `;

    const response2 = await graphqlClient(METAFIELD_PREDEFINED_VALUES_QUERY);
    const definition =
      response2?.data?.metafieldDefinition ?? response2?.metafieldDefinition;

    const validations = definition?.validations || [];

    // Step 3: Extract current group names and append the new one
    const choicesValidation = validations.find((v) => v.name === "choices");
    let currentChoices = [];

    if (choicesValidation && choicesValidation.value) {
      try {
        currentChoices = JSON.parse(choicesValidation.value);
      } catch (e) {
        console.error(
          "Invalid choices JSON format, initializing to empty list."
        );
        currentChoices = []; // Safe fallback
      }
    }

    // Append the new "group_name" into the predefined choices list
    if (!currentChoices.includes(group_name)) {
      currentChoices.push(group_name);
    }

    // Step 4: Update metafield definition with new list
    const UPDATE_ENUM_VALUES_MUTATION = `
      mutation metafieldDefinitionUpdate($definition: MetafieldDefinitionUpdateInput!) {
        metafieldDefinitionUpdate(definition: $definition) {
          updatedDefinition {
            id
            validations {
              name
              value
            }
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const variables = {
      definition: {
        namespace: groupNameMetafield.namespace,
        key: groupNameMetafield.key,
        ownerType: "PRODUCT",
        validations: [
          {
            name: "choices",
            value: JSON.stringify(currentChoices),
          },
        ],
      },
    };

    const updateResponse = await graphqlClient(
      UPDATE_ENUM_VALUES_MUTATION,
      variables
    );

    // Check for update errors
    if (updateResponse?.errors) {
      console.error("GraphQL errors:", updateResponse.errors);
      throw new Error("GraphQL mutation failed");
    }

    // Check for user errors
    const userErrors =
      updateResponse?.data?.metafieldDefinitionUpdate?.userErrors;

    if (userErrors && userErrors.length > 0) {
      console.error("Shopify update errors:", userErrors);
      throw new Error("Failed to update Shopify group list");
    }

    /**
     * Insert the new group into the database
     */
    const [result] = await connection.execute(
      `INSERT INTO wedding_band.stlr_groups (group_name) VALUES (?)`,
      [group_name]
    );

    await connection.commit(); // Save the changes

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Group created successfully",
      result: result.insertId,
    });
  } catch (error) {
    console.error("Error creating group:", error);
    return res.status(500).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: "Internal Server Error",
    });
  } finally {
    connection.release(); // ✅ Always release the connection
  }
});

const fetchGroupMetafieldId = async (syncId) => {
  try {
    const GET_METAFIELDS_QUERY = `
        query ProductMetafields($ownerId: ID!) {
            product(id: $ownerId) {
                metafields(first: 50) {
                    edges {
                        node {
                            id
                            key
                        }
                    }
                }
            }
        }`;

    const response = await graphqlClient(GET_METAFIELDS_QUERY, {
      ownerId: syncId,
    });
    if (!response.success) return null;

    return (
      response?.data?.product?.metafields?.edges
        ?.map((edge) => edge.node)
        ?.find((metafield) => metafield.key === "group_name")?.id || null
    );
  } catch (error) {
    console.error(`Error fetching metafield ID for sync_id: ${syncId}`, error);
    return null;
  }
};

const updateShopifyMetafield = async (syncId, groupName) => {
  try {
    const metafieldId = await fetchGroupMetafieldId(syncId);

    if (!metafieldId) {
      console.log(`No 'group_name' metafield found for sync_id: ${syncId}`);
      return;
    }

    const UPDATE_META_MUTATION = `
        mutation updateProductMetafields($input: ProductInput!) {
            productUpdate(input: $input) {
                product {
                id
                }
                userErrors {
                message
                field
                }
            }
            }
          `;

    await graphqlClient(UPDATE_META_MUTATION, {
      input: {
        metafields: [
          {
            id: metafieldId,
            value: groupName,
          },
        ],
        id: syncId,
      },
    });
  } catch (error) {
    console.error(
      `Error updating Shopify metafield for sync_id: ${syncId}`,
      error
    );
  }
};

// Old Code; No Need;
// const deleteGroup = tryCatchFn(async (req, res) => {
//   const deleteGroupsSchema = Joi.object({
//     ids: Joi.array()
//       .items(
//         Joi.number().integer().positive().strict().required() // Enforce strict number type
//       )
//       .min(1)
//       .required()
//       .messages({
//         "array.base": "Group IDs must be an array.",
//         "array.min": "At least one group ID is required.",
//         "number.base": "Each Group ID must be a number.",
//         "number.integer": "Each Group ID must be an integer.",
//         "number.positive": "Each Group ID must be a positive number.",
//         "any.required": "Group IDs are required.",
//       }),
//   });

//   const { ids } = req.body;

//   // Validate request body
//   const { error } = deleteGroupsSchema.validate({ ids });
//   if (error) {
//     return res.status(400).json({
//       status: httpStatusCodes.BAD_REQUEST,
//       success: false,
//       message: error.details[0].message,
//     });
//   }

//   const connection = await sqldb.getConnection(); // Get connection from pool

//   try {
//     await connection.beginTransaction(); // Start transaction

//     // ✅ 1. Check if all provided group IDs exist
//     const [existingGroups] = await connection.execute(
//       `SELECT group_id FROM wedding_band.stlr_groups WHERE group_id IN (${ids
//         .map(() => "?")
//         .join(", ")})`,
//       ids
//     );

//     const existingGroupIds = new Set(existingGroups.map((g) => g.group_id));
//     const missingGroups = ids.filter((id) => !existingGroupIds.has(id));

//     if (missingGroups.length > 0) {
//       return res.status(404).json({
//         status: 404,
//         success: false,
//         message: `Some groups do not exist: ${missingGroups.join(", ")}`,
//       });
//     }

//     // ✅ 2. Check if any group is synced (rings with a non-empty sync_id)
//     const [syncedGroups] = await connection.execute(
//       `SELECT DISTINCT r.group_id
//              FROM wedding_band.stlr_ring_variations v
//              JOIN wedding_band.stlr_rings r ON r.ring_id = v.ring_id
//              WHERE r.group_id IN (${ids.map(() => "?").join(", ")})
//                AND v.sync_id IS NOT NULL AND v.sync_id != ''`,
//       ids
//     );

//     if (syncedGroups.length > 0) {
//       return res.status(400).json({
//         status: 400,
//         success: false,
//         message: `Some groups cannot be deleted as they are synced: ${syncedGroups
//           .map((g) => g.group_id)
//           .join(", ")}`,
//       });
//     }

//     // ✅ 3. Update `stlr_rings` table to set `group_id = 0` for affected rings
//     await connection.execute(
//       `UPDATE wedding_band.stlr_rings SET group_id = 0 WHERE group_id IN (${ids
//         .map(() => "?")
//         .join(", ")})`,
//       ids
//     );

//     // ✅ 4. Delete the groups
//     await connection.execute(
//       `DELETE FROM wedding_band.stlr_groups WHERE group_id IN (${ids
//         .map(() => "?")
//         .join(", ")})`,
//       ids
//     );

//     await connection.commit(); // Commit transaction

//     return res.status(200).json({
//       status: httpStatusCodes.OK,
//       success: true,
//       message: "Groups deleted successfully.",
//     });
//   } catch (error) {
//     await connection.rollback(); // Rollback in case of an error
//     console.error("Error deleting groups:", error);
//     return res.status(500).json({
//       status: httpStatusCodes.INTERNAL_SERVER,
//       success: false,
//       message: "Internal Server Error",
//     });
//   } finally {
//     connection.release(); // Release the connection back to the pool
//   }
// });

// Old Code; No Need;
// const updateGroupsBulk = tryCatchFn(async (req, res) => {
//   // ✅ 1. Joi Validation
//   const bulkUpdateSchema = Joi.array()
//     .min(1)
//     .items(
//       Joi.object({
//         group_id: Joi.number().integer().required().messages({
//           "number.base": "Group ID must be a number.",
//           "number.integer": "Group ID must be an integer.",
//           "any.required": "Group ID is required.",
//         }),
//         group_name: Joi.string()
//           .trim()
//           .min(2)
//           .max(100)
//           .regex(/^(?!\d+$).*$/)
//           .required()
//           .messages({
//             "string.empty": "Group name is required.",
//             "string.min": "Group name must be at least 2 characters long.",
//             "string.max": "Group name cannot exceed 100 characters.",
//             "string.pattern.base": "Group name cannot be only numbers.",
//           }),
//       }).unknown(true)
//     )
//     .required()
//     .messages({
//       "array.base": "Groups must be an array.",
//       "array.min": "At least one group must be provided.",
//       "any.required": "Groups array is required.",
//     });

//   const { error } = bulkUpdateSchema.validate(req.body);
//   if (error) {
//     return res.status(400).json({
//       status: httpStatusCodes.BAD_REQUEST,
//       success: false,
//       message: error.details[0].message,
//     });
//   }

//   const groupsToUpdate = req.body;
//   const groupIds = groupsToUpdate.map((group) => group.group_id);

//   // Helper: normalize group names (trim + lowercase)
//   const normalize = (name) => name.trim().toLowerCase();

//   // Check for duplicate group_name in input
//   const seenNames = new Set();
//   const duplicateNames = new Set();

//   for (const { group_name } of groupsToUpdate) {
//     const normalized = normalize(group_name);
//     if (seenNames.has(normalized)) {
//       duplicateNames.add(normalized);
//     } else {
//       seenNames.add(normalized);
//     }
//   }

//   if (duplicateNames.size > 0) {
//     return res.status(400).json({
//       status: httpStatusCodes.BAD_REQUEST,
//       success: false,
//       message: `Duplicate group name(s) in input: ${[...duplicateNames].join(
//         ", "
//       )}`,
//     });
//   }

//   const connection = await sqldb.getConnection();

//   try {
//     await connection.beginTransaction();

//     // ✅ 2. Validate group existence
//     const [groupCountResult] = await connection.execute(
//       `SELECT COUNT(*) AS count FROM wedding_band.stlr_groups
//              WHERE group_id IN (${groupIds.map(() => "?").join(", ")})`,
//       groupIds
//     );

//     if (groupCountResult[0].count !== groupIds.length) {
//       return res.status(404).json({
//         status: 404,
//         success: false,
//         message: "Some groups do not exist.",
//       });
//     }

//     // ✅ 3. Check for duplicate group names (excluding current groups)
//     const [duplicateGroups] = await connection.execute(
//       `SELECT group_name FROM wedding_band.stlr_groups
//              WHERE group_name IN (${groupsToUpdate.map(() => "?").join(", ")})
//              AND group_id NOT IN (${groupIds.map(() => "?").join(", ")})`,
//       [...groupsToUpdate.map((g) => g.group_name), ...groupIds]
//     );

//     if (duplicateGroups.length > 0) {
//       return res.status(409).json({
//         status: 409,
//         success: false,
//         message: `Some group names already exist: ${duplicateGroups
//           .map((d) => d.group_name)
//           .join(", ")}`,
//       });
//     }

//     // ✅ 4. Check if any group has synced rings
//     const [syncedGroups] = await connection.execute(
//       `SELECT DISTINCT group_id FROM wedding_band.stlr_rings
//              WHERE group_id IN (${groupIds.map(() => "?").join(", ")})
//              AND ring_id IN (
//                  SELECT ring_id FROM wedding_band.stlr_ring_variations
//                  WHERE sync_id IS NOT NULL AND sync_id != ''
//              )`,
//       groupIds
//     );

//     if (syncedGroups.length > 0) {
//       // ✅ 5. If only one group exists and has synced rings, update Shopify metafield
//       if (groupIds.length === 1) {
//         const [checkExistGroupName] = await connection.query(
//           "SELECT group_name FROM stlr_groups WHERE group_id = ?",
//           [groupIds[0]]
//         );

//         const existingGroupName =
//           checkExistGroupName.length > 0
//             ? checkExistGroupName[0].group_name
//             : ""; // Use group_name if exists, otherwise empty string

//         const [variations] = await connection.execute(
//           `SELECT sync_id FROM wedding_band.stlr_ring_variations
//                      WHERE ring_id IN (SELECT ring_id FROM wedding_band.stlr_rings WHERE group_id = ?)
//                      AND sync_id IS NOT NULL AND sync_id != ''`,
//           [groupIds[0]]
//         );

//         for (const { sync_id } of variations) {
//           await updateShopifyMetafield(sync_id, groupsToUpdate[0].group_name);
//         }

//         await connection.execute(
//           `UPDATE wedding_band.stlr_groups SET group_name = ? WHERE group_id = ?`,
//           [groupsToUpdate[0].group_name, groupsToUpdate[0].group_id]
//         );

//         await connection.commit();
//         return res.status(200).json({
//           status: httpStatusCodes.OK,
//           success: true,
//           message: "Group updated successfully.",
//         });
//       }

//       // If multiple groups have synced rings, prevent update
//       return res.status(409).json({
//         status: 409,
//         success: false,
//         message: "Some groups cannot be updated as their rings are synced.",
//         result: syncedGroups.map((v) => v.group_id),
//       });
//     }

//     // ✅ 6. Perform bulk update
//     const updateQuery = `
//             UPDATE wedding_band.stlr_groups
//             SET group_name = CASE
//                 ${groupsToUpdate
//                   .map(() => "WHEN group_id = ? THEN ?")
//                   .join(" ")}
//             END
//             WHERE group_id IN (${groupIds.map(() => "?").join(", ")});
//         `;

//     const updateValues = [
//       ...groupsToUpdate.flatMap((group) => [group.group_id, group.group_name]),
//       ...groupIds,
//     ];

//     await connection.execute(updateQuery, updateValues);
//     await connection.commit();

//     return res.status(200).json({
//       status: httpStatusCodes.OK,
//       success: true,
//       message: "Groups updated successfully.",
//     });
//   } catch (error) {
//     await connection.rollback();
//     console.error("Error updating groups:", error);
//     return res.status(500).json({
//       status: httpStatusCodes.INTERNAL_SERVER,
//       success: false,
//       message: "Internal Server Error",
//     });
//   } finally {
//     connection.release();
//   }
// });

const getShopifyCollectionList = async () => {
  try {
    const GET_COLLECTIONS_LIST_QUERY = `
      query CustomCollectionList {
        collections(first: 250, sortKey: TITLE, query: "collection_type:custom") {
          nodes {
            id
            title
          }
        }
      }
    `;

    const shopifyResponse = await graphqlClient(GET_COLLECTIONS_LIST_QUERY);
    if (!shopifyResponse.success) return [];

    return shopifyResponse.data.collections.nodes.map(({ id, title }) => ({
      shopify_id: id.toString(),
      name: title,
      slug: title.toLowerCase().replace(/\s+/g, "-"),
    }));
  } catch (error) {
    console.error("Error fetching Shopify collections:", error);
    return [];
  }
};

const syncCategory = tryCatchFn(async (req, res) => {
  const connection = await sqldb.getConnection(); // Get connection from pool

  try {
    await connection.beginTransaction(); // Start transaction

    // Fetch categories from Shopify
    const shopifyCategories = await getShopifyCollectionList();

    // Fetch existing categories from DB
    const [dbCategories] = await connection.execute(
      `SELECT * FROM wedding_band.stlr_categories`
    );

    const dbCategoryMap = new Map(
      dbCategories.map((cat) => [cat.shopify_id, cat])
    );

    const categoriesToInsert = [];
    const categoriesToUpdate = [];
    const categoriesToDelete = [];

    // Compare Shopify data with DB
    for (const category of shopifyCategories) {
      if (!dbCategoryMap.has(category.shopify_id)) {
        // New category, insert it
        categoriesToInsert.push(category);
      } else {
        // Check if modification is needed
        const existing = dbCategoryMap.get(category.shopify_id);
        if (
          existing.name !== category.name ||
          existing.slug !== category.slug
        ) {
          categoriesToUpdate.push(category);
        }
        dbCategoryMap.delete(category.shopify_id); // Remove from map
      }
    }

    // Remaining items in dbCategoryMap are deleted categories
    categoriesToDelete.push(...dbCategoryMap.values());

    // Perform bulk INSERT
    if (categoriesToInsert.length > 0) {
      const insertValues = categoriesToInsert.map((cat) => [
        cat.name,
        cat.slug,
        cat.shopify_id,
      ]);
      await connection.query(
        `INSERT INTO wedding_band.stlr_categories (name, slug, shopify_id) VALUES ?`,
        [insertValues]
      );
    }

    // Perform bulk UPDATE using CASE WHEN
    if (categoriesToUpdate.length > 0) {
      const updateQuery = `
                UPDATE wedding_band.stlr_categories
                SET name = CASE shopify_id
                    ${categoriesToUpdate
                      .map((cat) => `WHEN '${cat.shopify_id}' THEN ?`)
                      .join(" ")}
                END,
                slug = CASE shopify_id
                    ${categoriesToUpdate
                      .map((cat) => `WHEN '${cat.shopify_id}' THEN ?`)
                      .join(" ")}
                END
                WHERE shopify_id IN (${categoriesToUpdate
                  .map((cat) => `'${cat.shopify_id}'`)
                  .join(", ")});
            `;
      await connection.execute(updateQuery, [
        ...categoriesToUpdate.map((cat) => cat.name),
        ...categoriesToUpdate.map((cat) => cat.slug),
      ]);
    }

    // Handle deleted categories in one request
    if (categoriesToDelete.length > 0) {
      const categoryIds = categoriesToDelete.map((cat) => cat.category_id);

      // Set category_id = 0 in stlr_rings
      await connection.execute(
        `UPDATE wedding_band.stlr_rings SET category_id = 0 WHERE category_id IN (${categoryIds.join(
          ", "
        )})`
      );

      // Delete categories in bulk
      await connection.execute(
        `DELETE FROM wedding_band.stlr_categories WHERE category_id IN (${categoryIds.join(
          ", "
        )})`
      );
    }

    await connection.commit(); // Commit transaction

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Categories synced successfully",
      inserted: categoriesToInsert.length,
      updated: categoriesToUpdate.length,
      deleted: categoriesToDelete.length,
    });
  } catch (error) {
    await connection.rollback(); // Rollback in case of an error
    console.error("Error syncing categories:", error);
    return res
      .status(500)
      .json({ success: false, message: "Internal Server Error" });
  } finally {
    connection.release(); // Release the connection back to the pool
  }
});

const getCategoryList = tryCatchFn(async (req, res) => {
  try {
    // Fetch categories from Shopify
    const shopifyCategories = await getShopifyCollectionList();

    // ✅ 2. Fetch categories from MySQL
    const query = `SELECT * FROM stlr_categories ORDER BY name ASC`;
    const [mysqlCategories] = await sqldb.query(query);

    const combinedCategories = {
      shopifyCategoryList: [...shopifyCategories],
      mysqlCategoryList: [...mysqlCategories],
    };

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Categories fetched successfully.",
      result: combinedCategories,
    });
  } catch (error) {
    console.error("Error fetching category list:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
});

const updateSingleGroup = async (req, res) => {
  const updateGroupSchema = Joi.object({
    id: Joi.number().integer().positive().required().messages({
      "number.base": "Group ID must be a number.",
      "number.integer": "Group ID must be an integer.",
      "number.positive": "Group ID must be a positive number.",
      "any.required": "Group ID is required.",
    }),
  });

  const groupNameSchema = Joi.object({
    group_name: Joi.string()
      .trim()
      .min(2)
      .max(100)
      .regex(/^(?!\d+$).*$/)
      .required()
      .messages({
        "string.empty": "Group name is required.",
        "string.min": "Group name must be at least 2 characters long.",
        "string.max": "Group name cannot exceed 100 characters.",
        "string.pattern.base": "Group name cannot be only numbers.",
      }),
  });

  const { id } = req.params;
  const numericId = Number(id);

  const { error: idError } = updateGroupSchema.validate({ id: numericId });
  if (idError) {
    return res.status(400).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: idError.details[0].message,
    });
  }

  const { error: nameError } = groupNameSchema.validate(req.body);
  if (nameError) {
    return res.status(400).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: nameError.details[0].message,
    });
  }

  const { group_name } = req.body;

  const connection = await sqldb.getConnection();

  try {
    await connection.beginTransaction();

    // Check if group exists
    const [groups] = await connection.query(
      "SELECT * FROM stlr_groups WHERE group_id = ? LIMIT 1",
      [numericId]
    );

    if (groups.length === 0) {
      await connection.rollback();
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Group not found. Please provide a valid group ID.",
      });
    }

    const oldGroupName = groups[0].group_name;

    // Check for duplicate group name
    const [existingGroup] = await connection.query(
      "SELECT group_id FROM stlr_groups WHERE group_name = ? AND group_id != ? LIMIT 1",
      [group_name, numericId]
    );

    if (existingGroup.length > 0) {
      await connection.rollback();
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: "Group name already exists. Please choose a different name.",
      });
    }

    /**
     * Update the group name in the predefined group names list.
     */
    // Get the Group Name metafield id
    const METAFIELD_DEFINITIONS_QUERY = `
      query {
        metafieldDefinitions(first: 100, ownerType: PRODUCT) {
          edges {
            node {
              id
              name
              namespace
              key
              validations {
                name
                value
              }
            }
          }
        }
      }
    `;

    const response1 = await graphqlClient(METAFIELD_DEFINITIONS_QUERY);
    const definitions = response1?.data?.metafieldDefinitions?.edges?.map(
      (edge) => edge.node
    );
    const groupNameMetafield = definitions.find(
      (def) => def.name === "Group Name"
    );

    if (!groupNameMetafield) {
      throw new Error("Group Name metafield definition not found");
    }

    const groupNameMetafieldId = groupNameMetafield.id;

    // Get the existing group names from the metafield "validations"
    const METAFIELD_PREDEFINED_VALUES_QUERY = `
      query {
        metafieldDefinition(id: "${groupNameMetafieldId}") {
          id
          name
          namespace
          key
          validations {
            name
            value
          }
        }
      }
    `;

    const response2 = await graphqlClient(METAFIELD_PREDEFINED_VALUES_QUERY);
    const definition =
      response2?.data?.metafieldDefinition ?? response2?.metafieldDefinition;
    const validations = definition?.validations || [];

    const choicesValidation = validations.find((v) => v.name === "choices");
    let currentChoices = [];

    if (choicesValidation && choicesValidation.value) {
      try {
        currentChoices = JSON.parse(choicesValidation.value);
      } catch (e) {
        currentChoices = [];
      }
    }

    // Replace the old name with the new group name in the choices array
    let updatedChoices = currentChoices.map((choice) =>
      choice === oldGroupName ? group_name : choice
    );

    updatedChoices = [...new Set(updatedChoices)];

    // Update the metafield definition with the new list of group names
    const UPDATE_ENUM_VALUES_MUTATION = `
      mutation metafieldDefinitionUpdate($definition: MetafieldDefinitionUpdateInput!) {
        metafieldDefinitionUpdate(definition: $definition) {
          updatedDefinition {
            id
            validations {
              name
              value
            }
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const variables = {
      definition: {
        namespace: groupNameMetafield.namespace,
        key: groupNameMetafield.key,
        ownerType: "PRODUCT",
        validations: [
          {
            name: "choices",
            value: JSON.stringify(updatedChoices),
          },
        ],
      },
    };

    const updateResponse = await graphqlClient(
      UPDATE_ENUM_VALUES_MUTATION,
      variables
    );

    if (updateResponse?.errors) {
      throw new Error("GraphQL mutation failed");
    }

    const userErrors =
      updateResponse?.data?.metafieldDefinitionUpdate?.userErrors;

    if (userErrors && userErrors.length > 0) {
      throw new Error("Failed to update Shopify group list");
    }

    /**
     *  All the products which were present in the shopify with old group name should update with the new group name
     */
    // Get all the products which have metafield "Group Name" and "Group Name is equal to oldGroupName"
    const PRODUCT_WITH_GROUP_NAME_QUERY = `
      query {
        products(first: 100) {
          edges {
            node {
              id
              title
              metafields(first: 10, namespace: "custom") {
                edges {
                  node {
                    key
                    value
                  }
                }
              }
            }
          }
        }
      }
    `;

    const productCheckResponse = await graphqlClient(
      PRODUCT_WITH_GROUP_NAME_QUERY
    );

    const productEdges = productCheckResponse?.data?.products?.edges || [];

    const productsToUpdate = productEdges.filter((edge) => {
      const metafields = edge.node.metafields?.edges?.map((m) => m.node) || [];
      return metafields.some(
        (mf) =>
          mf.key === "group_name" && mf.value.trim() === oldGroupName.trim()
      );
    });

    console.log("products to update is", productsToUpdate);

    // Update the products and maintain two arrays one for successful updates and one for failed updates.
    // If we get failed updates after the update process, then revert all updated products and throw error.
    if (productsToUpdate.length > 0) {
      const successfulUpdates = [];
      const failedUpdates = [];

      for (const product of productsToUpdate) {
        const updateMutation = `
          mutation {
            metafieldsSet(metafields: [{
              namespace: "custom",
              key: "group_name",
              ownerId: "${product.node.id}",
              value: "${group_name}",
              type: "single_line_text_field"
            }]) {
              metafields {
                id
                key
                value
              }
              userErrors {
                field
                message
              }
            }
          }
        `;

        try {
          const updateResponse = await graphqlClient(updateMutation);
          const errors = updateResponse?.data?.metafieldsSet?.userErrors || [];

          if (errors.length > 0) {
            console.error(
              `Failed to update product ${product.node.title}:`,
              errors
            );
            failedUpdates.push(product);
          } else {
            console.log(`Successfully updated product: ${product.node.title}`);
            successfulUpdates.push(product);
          }
        } catch (err) {
          console.error(
            `Error updating product ${product.node.title}:`,
            err.message
          );
          failedUpdates.push(product);
        }
      }

      // If there are failed updates, revert successful updates and throw error
      if (failedUpdates.length > 0) {
        for (const successfulProduct of successfulUpdates) {
          const revertMutation = `
            mutation {
              metafieldsSet(metafields: [{
                namespace: "custom",
                key: "group_name",
                ownerId: "${successfulProduct.node.id}",
                value: "${oldGroupName}",
                type: "single_line_text_field"
              }]) {
                metafields {
                  id
                  key
                  value
                }
                userErrors {
                  field
                  message
                }
              }
            }
          `;

          try {
            await graphqlClient(revertMutation);
            console.log(
              `Reverted product ${successfulProduct.node.title} to old group name.`
            );
          } catch (err) {
            console.error(
              `Error reverting product ${successfulProduct.node.title}:`,
              err.message
            );
          }
        }

        const failedProductTitles = failedUpdates
          .map((p) => p.node.title)
          .join(", ");
        console.error(
          `Failed to update the following products: ${failedProductTitles}`
        );

        throw new Error(
          `Failed to update some products. Rolled back changes. Please check the logs.`
        );
      } else {
        console.log("All products updated successfully.");
      }
    }

    /**
     *  Perform the final update in the stlr_groups table in the DB
     */
    await connection.query(
      "UPDATE stlr_groups SET group_name = ? WHERE group_id = ?",
      [group_name, numericId]
    );

    await connection.commit();

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Group updated successfully.",
    });
  } catch (error) {
    await connection.rollback();
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  } finally {
    connection.release();
  }
};

const deleteSingleGroup = async (req, res) => {
  const deleteGroupSchema = Joi.object({
    id: Joi.number().integer().positive().required().messages({
      "number.base": "Group ID must be a number.",
      "number.integer": "Group ID must be an integer.",
      "number.positive": "Group ID must be a positive number.",
      "any.required": "Group ID is required.",
    }),
  });

  const { id } = req.params;
  const numericId = Number(id);

  const { error: idError } = deleteGroupSchema.validate({ id: numericId });

  if (idError) {
    return res.status(400).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: idError.details[0].message,
    });
  }

  const connection = await sqldb.getConnection();

  try {
    await connection.beginTransaction();

    // Check if group exists
    const [groups] = await connection.query(
      "SELECT * FROM stlr_groups WHERE group_id = ? LIMIT 1",
      [numericId]
    );

    if (groups.length === 0) {
      await connection.rollback();
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Group not found. Please provide a valid group ID.",
      });
    }

    const groupName = groups[0].group_name;

    /**
     *  Check whether any product has this group name or not. If any product has this group name, then return error.
     */
    const PRODUCT_WITH_GROUP_NAME_QUERY = `
      query {
        products(first: 100) {
          edges {
            node {
              id
              title
              metafields(first: 10, namespace: "custom") {
                edges {
                  node {
                    key
                    value
                  }
                }
              }
            }
          }
        }
      }
    `;

    const productCheckResponse = await graphqlClient(
      PRODUCT_WITH_GROUP_NAME_QUERY
    );
    const productEdges = productCheckResponse?.data?.products?.edges || [];

    const usedProduct = productEdges.find((edge) => {
      const metafields = edge.node.metafields?.edges?.map((m) => m.node) || [];
      return metafields.some(
        (mf) => mf.key === "group_name" && mf.value.trim() === groupName.trim()
      );
    });

    if (usedProduct) {
      await connection.rollback();
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: `Cannot delete group '${groupName}' because it is used in one or more products.`,
      });
    }

    /**
     *  If there is no product with this group name, then delete the group name from the predefined metafield validations.
     */
    // Get the Group Name metafield id
    const METAFIELD_DEFINITIONS_QUERY = `
      query {
        metafieldDefinitions(first: 100, ownerType: PRODUCT) {
          edges {
            node {
              id
              name
              namespace
              key
              validations {
                name
                value
              }
            }
          }
        }
      }
    `;

    const response1 = await graphqlClient(METAFIELD_DEFINITIONS_QUERY);
    const definitions = response1?.data?.metafieldDefinitions?.edges?.map(
      (edge) => edge.node
    );
    const groupNameMetafield = definitions.find(
      (def) => def.name === "Group Name"
    );

    if (!groupNameMetafield) {
      throw new Error("Group Name metafield definition not found");
    }

    const groupNameMetafieldId = groupNameMetafield.id;

    // Get the existing group names from the metafield "validations"
    const METAFIELD_PREDEFINED_VALUES_QUERY = `
      query {
        metafieldDefinition(id: "${groupNameMetafieldId}") {
          id
          name
          namespace
          key
          validations {
            name
            value
          }
        }
      }
    `;

    const response2 = await graphqlClient(METAFIELD_PREDEFINED_VALUES_QUERY);
    const definition =
      response2?.data?.metafieldDefinition ?? response2?.metafieldDefinition;
    const validations = definition?.validations || [];

    const choicesValidation = validations.find((v) => v.name === "choices");
    let currentChoices = [];

    if (choicesValidation && choicesValidation.value) {
      try {
        currentChoices = JSON.parse(choicesValidation.value);
      } catch (e) {
        currentChoices = [];
      }
    }

    // Remove the deleted group name from the choices array
    const updatedChoices = currentChoices.filter(
      (choice) => choice !== groupName
    );

    // Update the metafield definition with the new list of group names
    const UPDATE_ENUM_VALUES_MUTATION = `
      mutation metafieldDefinitionUpdate($definition: MetafieldDefinitionUpdateInput!) {
        metafieldDefinitionUpdate(definition: $definition) {
          updatedDefinition {
            id
            validations {
              name
              value
            }
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const variables = {
      definition: {
        namespace: groupNameMetafield.namespace,
        key: groupNameMetafield.key,
        ownerType: "PRODUCT",
        validations: [
          {
            name: "choices",
            value: JSON.stringify(updatedChoices),
          },
        ],
      },
    };

    const updateResponse = await graphqlClient(
      UPDATE_ENUM_VALUES_MUTATION,
      variables
    );

    if (updateResponse?.errors) {
      throw new Error("GraphQL mutation failed");
    }

    const userErrors =
      updateResponse?.data?.metafieldDefinitionUpdate?.userErrors;

    if (userErrors && userErrors.length > 0) {
      throw new Error("Failed to update Shopify group list");
    }

    /**
     *  Update the table stlr_rings and then delete the group from the table stlr_groups.
     */
    await connection.execute(
      `UPDATE wedding_band.stlr_rings SET group_id = 0 WHERE group_id = ?`,
      [numericId]
    );

    await connection.execute(
      `DELETE FROM wedding_band.stlr_groups WHERE group_id = ?`,
      [numericId]
    );

    await connection.commit();

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Group deleted successfully.",
    });
  } catch (error) {
    await connection.rollback();
    console.error("Delete group error:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

export {
  getGroup,
  createGroup,
  syncCategory,
  getGroupsByIds,
  getCategoryList,
  updateSingleGroup,
  deleteSingleGroup,
};
