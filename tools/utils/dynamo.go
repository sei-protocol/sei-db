package utils

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// DynamoDBClient wraps the DynamoDB service with common operations
type DynamoDBClient struct {
	client *dynamodb.DynamoDB
	table  string
}

// NewDynamoDBClient creates a new DynamoDB client
func NewDynamoDBClient(tableName, awsRegion string) (*DynamoDBClient, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(awsRegion),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}

	return &DynamoDBClient{
		client: dynamodb.New(sess),
		table:  tableName,
	}, nil
}

// StateSizeAnalysis represents the analysis data to be stored in DynamoDB
type StateSizeAnalysis struct {
	BlockHeight       int64  `json:"block_height" dynamodbav:"block_height"`
	ModuleName        string `json:"module_name" dynamodbav:"module_name"`
	TotalNumKeys      uint64 `json:"total_num_keys" dynamodbav:"total_num_keys"`
	TotalKeySize      uint64 `json:"total_key_size" dynamodbav:"total_key_size"`
	TotalValueSize    uint64 `json:"total_value_size" dynamodbav:"total_value_size"`
	TotalSize         uint64 `json:"total_size" dynamodbav:"total_size"`
	PrefixBreakdown   string `json:"prefix_breakdown" dynamodbav:"prefix_breakdown"`
	ContractBreakdown string `json:"contract_breakdown" dynamodbav:"contract_breakdown"`
}

// ContractSizeEntry represents individual contract size data
type ContractSizeEntry struct {
	Address   string `json:"address"`
	TotalSize uint64 `json:"total_size"`
	KeyCount  uint64 `json:"key_count"`
}

// PrefixSize is a helper structure kept for completeness (unused here)
type PrefixSize struct {
	KeySize   uint64 `json:"key_size"`
	ValueSize uint64 `json:"value_size"`
	TotalSize uint64 `json:"total_size"`
	KeyCount  uint64 `json:"key_count"`
}

// ExportStateSizeAnalysis exports a single module analysis to DynamoDB
func (d *DynamoDBClient) ExportStateSizeAnalysis(analysis *StateSizeAnalysis) error {
	// Convert to DynamoDB attribute values
	item, err := dynamodbattribute.MarshalMap(analysis)
	if err != nil {
		return fmt.Errorf("failed to marshal analysis for module %s: %w", analysis.ModuleName, err)
	}

	// Write to DynamoDB
	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(d.table),
	}

	_, err = d.client.PutItem(input)
	if err != nil {
		return fmt.Errorf("failed to write to DynamoDB for module %s: %w", analysis.ModuleName, err)
	}

	return nil
}

// ExportMultipleAnalyses exports multiple analyses sequentially
func (d *DynamoDBClient) ExportMultipleAnalyses(analyses []*StateSizeAnalysis) error {
	for i, analysis := range analyses {
		if err := d.ExportStateSizeAnalysis(analysis); err != nil {
			return fmt.Errorf("failed to export analysis %d (module: %s): %w", i, analysis.ModuleName, err)
		}
	}
	return nil
}

// UpdateLatestHeightIfGreater keeps exactly one item in the metadata table
// Table schema: Partition key 'keyname' (S), Sort key 'height' (N)
// We persist exactly one item with key: keyname = "latest_height", height = 0
// and store the latest value in a non-key attribute 'latest_height'.
// This avoids attempting to update key attributes (not allowed in DynamoDB).
func (d *DynamoDBClient) UpdateLatestHeightIfGreater(metadataTable string, height int64) (bool, error) {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(metadataTable),
		Key: map[string]*dynamodb.AttributeValue{
			"keyname": {S: aws.String("latest_height")},
			"height":  {N: aws.String("0")},
		},
		UpdateExpression:    aws.String("SET latest_height = :h"),
		ConditionExpression: aws.String("attribute_not_exists(latest_height) OR latest_height < :h"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":h": {N: aws.String(fmt.Sprintf("%d", height))},
		},
		ReturnValues: aws.String("NONE"),
	}

	_, err := d.client.UpdateItem(input)
	if err != nil {
		var aerr awserr.Error
		if errors.As(err, &aerr) && aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return false, nil
		}
		return false, fmt.Errorf("failed to update latest height: %w", err)
	}
	fmt.Printf("Updated Dynamodb with latest height %d\n", height)
	return true, nil
}
