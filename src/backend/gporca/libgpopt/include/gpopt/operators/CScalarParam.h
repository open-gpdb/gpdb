//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 Broadcom
//
//	@filename:
//		CScalarParam.h
//
//	@doc:
//		Scalar paramater
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarParam_H
#define GPOPT_CScalarParam_H

#include "gpos/base.h"

#include "gpopt/operators/CScalar.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarParam
//
//	@doc:
//		scalar parameter
//
//---------------------------------------------------------------------------
class CScalarParam : public CScalar
{
private:
	// param id
	const ULONG m_id;

	// param type
	IMDId *m_type;

	// param type modifier
	INT m_type_modifier;

	CScalarParam(const CScalarParam &);

public:
	// ctor
	CScalarParam(CMemoryPool *mp, ULONG id, IMDId *type, INT type_modifier)
		: CScalar(mp), m_id(id), m_type(type), m_type_modifier(type_modifier)
	{
	}

	// dtor
	virtual ~CScalarParam();

	// param accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopScalarParam;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CScalarParam";
	}

	// operator specific hash function
	virtual ULONG HashValue() const;

	ULONG
	Id() const
	{
		return m_id;
	}
	// the type of the scalar expression
	virtual IMDId *
	MdidType() const
	{
		return m_type;
	}

	// the type modifier of the scalar expression
	virtual INT
	TypeModifier() const
	{
		return m_type_modifier;
	}

	// match function
	virtual BOOL Matches(COperator *pop) const;


	virtual BOOL
	FInputOrderSensitive() const
	{
		GPOS_ASSERT(!"Unexpected call of function FInputOrderSensitive");
		return false;
	}

	// return a copy of the operator with remapped columns
	virtual COperator *
	PopCopyWithRemappedColumns(CMemoryPool *,		//mp,
							   UlongToColRefMap *,	//colref_mapping,
							   BOOL					//must_exist
	)
	{
		return PopCopyDefault();
	}


	static BOOL
	Equals(const CScalarParam *left, const CScalarParam *right)
	{
		return left->Id() == right->Id();
	}


	// conversion function
	static CScalarParam *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopScalarParam == pop->Eopid());

		return dynamic_cast<CScalarParam *>(pop);
	}

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CScalarParam

}  // namespace gpopt


#endif	// !GPOPT_CScalarParam_H

// EOF
