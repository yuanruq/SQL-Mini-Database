/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.AnnotationTypeElementDoc;
import com.sun.javadoc.AnnotationValue;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.MethodDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.ParamTag;
import com.sun.javadoc.Parameter;
import com.sun.javadoc.SeeTag;
import com.sun.javadoc.SourcePosition;
import com.sun.javadoc.Tag;
import com.sun.javadoc.ThrowsTag;
import com.sun.javadoc.Type;
import com.sun.javadoc.TypeVariable;

class HidingAnnotationTypeElementDocWrapper extends HidingWrapper
    implements AnnotationTypeElementDoc {
  
    public HidingAnnotationTypeElementDocWrapper(
        AnnotationTypeElementDoc memdoc, Map mapWrappers) {
        super(memdoc, mapWrappers);
    }

    private AnnotationTypeElementDoc _getAnnotationTypeElementDoc() {
        return (AnnotationTypeElementDoc)getWrappedObject();
    }

    public AnnotationValue defaultValue() {
        return (AnnotationValue)
                wrapOrHide(_getAnnotationTypeElementDoc().defaultValue());
    }
  
    public boolean isAbstract() {
        return _getAnnotationTypeElementDoc().isAbstract();
    }

    public ClassDoc overriddenClass() {
        return (ClassDoc)
                wrapOrHide(_getAnnotationTypeElementDoc().overriddenClass());
    }

    public MethodDoc overriddenMethod() {
        return (MethodDoc)
                wrapOrHide(_getAnnotationTypeElementDoc().overriddenMethod());
    }

    public Type returnType() {
        return (Type)wrapOrHide(_getAnnotationTypeElementDoc().returnType());
    }

    public boolean overrides(MethodDoc meth) {
        if (meth instanceof HidingMethodDocWrapper) {
            meth  = (MethodDoc)
                    ((HidingMethodDocWrapper)meth).getWrappedObject();
        }
     
        return _getAnnotationTypeElementDoc().overrides((MethodDoc) meth);
    }

    public Type overriddenType() {
        return (Type)
                wrapOrHide(_getAnnotationTypeElementDoc().overriddenType());
    }

    public boolean isVarAgrs() {
        return _getAnnotationTypeElementDoc().isVarArgs();
    }

    public Type[] thrownExceptionTypes() {
        return (Type[])wrapOrHide(_getAnnotationTypeElementDoc()
                                  .thrownExceptionTypes());
    }
   
    public String flatSignature() {
        return _getAnnotationTypeElementDoc().flatSignature();
    }

    public boolean isNative() {
        return _getAnnotationTypeElementDoc().isNative();
    }

    public boolean isSynchronized() {
        return _getAnnotationTypeElementDoc().isSynchronized();
    }

    public ParamTag[] paramTags() {
        return (ParamTag[])
                wrapOrHide(_getAnnotationTypeElementDoc().paramTags());
    }

    public Parameter[] parameters() {
        return (Parameter[])
                wrapOrHide(_getAnnotationTypeElementDoc().parameters());
    }

    public String signature() {
        return _getAnnotationTypeElementDoc().signature();
    }

    public ClassDoc[] thrownExceptions() {
        return (ClassDoc[])wrapOrHide(_getAnnotationTypeElementDoc()
                                      .thrownExceptions());
    }

    public ThrowsTag[] throwsTags() {
        return (ThrowsTag[])
                wrapOrHide(_getAnnotationTypeElementDoc().throwsTags());
    }

    public TypeVariable[] typeParameters() {
        return (TypeVariable[])
                wrapOrHide(_getAnnotationTypeElementDoc().typeParameters());
    }

    public ParamTag[] typeParamTags() {
        return (ParamTag[])
                wrapOrHide(_getAnnotationTypeElementDoc().typeParamTags());
    }

    public boolean isVarArgs() {
        return _getAnnotationTypeElementDoc().isVarArgs();
    }

    public String commentText() {
        return _getAnnotationTypeElementDoc().commentText();
    }

    public int compareTo(Object obj) {
        if (obj instanceof HidingWrapper) {
            return _getAnnotationTypeElementDoc().
                    compareTo(((HidingWrapper)obj).getWrappedObject());
        } else {
            return _getAnnotationTypeElementDoc().compareTo(obj);
        }
    }

    public Tag[] firstSentenceTags() {
        return (Tag[])wrapOrHide(_getAnnotationTypeElementDoc()
                                 .firstSentenceTags());
    }

    public String getRawCommentText() {
        return _getAnnotationTypeElementDoc().getRawCommentText();
    }

    public Tag[] inlineTags() {
        return (Tag[])wrapOrHide(_getAnnotationTypeElementDoc().inlineTags());
    }

    public boolean isClass() {
        return _getAnnotationTypeElementDoc().isClass();
    }

    public boolean isConstructor() {
        return _getAnnotationTypeElementDoc().isConstructor();
    }

    public boolean isError() {
        return _getAnnotationTypeElementDoc().isError();
    }

    public boolean isException() {
        return _getAnnotationTypeElementDoc().isException();
    }

    public boolean isField() {
        return _getAnnotationTypeElementDoc().isField();
    }

    public boolean isIncluded() {
        return _getAnnotationTypeElementDoc().isIncluded();
    }

    public boolean isInterface() {
        return _getAnnotationTypeElementDoc().isInterface();
    }

    public boolean isMethod() {
        return _getAnnotationTypeElementDoc().isMethod();
    }

    public boolean isOrdinaryClass() {
        return _getAnnotationTypeElementDoc().isOrdinaryClass();
    }

    public String name() {
        return _getAnnotationTypeElementDoc().name();
    }

    public SeeTag[] seeTags() {
        return (SeeTag[])wrapOrHide(_getAnnotationTypeElementDoc().seeTags());
    }

    public void setRawCommentText(String szText) {
        _getAnnotationTypeElementDoc().setRawCommentText(szText);
    }

    public Tag[] tags() {
        return (Tag[])wrapOrHide(_getAnnotationTypeElementDoc().tags());
    }

    public Tag[] tags(String szTagName) {
        return (Tag[])
                wrapOrHide(_getAnnotationTypeElementDoc().tags(szTagName));
    }

    public SourcePosition position() {
        return (SourcePosition)
                wrapOrHide(_getAnnotationTypeElementDoc().position());
    }

    public boolean isAnnotationType() {
        return _getAnnotationTypeElementDoc().isAnnotationType();
    }
   
    public boolean isSynthetic() {
        return _getAnnotationTypeElementDoc().isSynthetic();
    }

    public AnnotationDesc[] annotations() {
        return (AnnotationDesc[])
                wrapOrHide(_getAnnotationTypeElementDoc().annotations());
    }

    public boolean isEnum() {
        return _getAnnotationTypeElementDoc().isEnum();
    }

    public boolean isAnnotationTypeElement() {
        return _getAnnotationTypeElementDoc().isAnnotationTypeElement();
    }

    public boolean isEnumConstant() {
        return _getAnnotationTypeElementDoc().isEnumConstant();
    }
     
    public boolean isFinal() {
        return _getAnnotationTypeElementDoc().isFinal();
    }

    public ClassDoc containingClass() {
        return (ClassDoc)
                wrapOrHide(_getAnnotationTypeElementDoc().containingClass());
    }

    public PackageDoc containingPackage() {
        return (PackageDoc)
                wrapOrHide(_getAnnotationTypeElementDoc().containingPackage());
    }

    public boolean isPackagePrivate() {
        return _getAnnotationTypeElementDoc().isPackagePrivate();
    }

    public boolean isPrivate() {
        return _getAnnotationTypeElementDoc().isPrivate();
    }

    public boolean isProtected() {
        return _getAnnotationTypeElementDoc().isProtected();
    }

    public boolean isPublic() {
        return _getAnnotationTypeElementDoc().isPublic();
    }

    public boolean isStatic() {
        return _getAnnotationTypeElementDoc().isStatic();
    }

    public int modifierSpecifier() {
        return _getAnnotationTypeElementDoc().modifierSpecifier();
    }

    public String modifiers() {
        return _getAnnotationTypeElementDoc().modifiers();
    }

    public String qualifiedName() {
        return _getAnnotationTypeElementDoc().qualifiedName();
    }
}
